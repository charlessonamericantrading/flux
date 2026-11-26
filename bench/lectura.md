<!-- ejecucion: 2026-11-26T18:53:03.690Z -->
**Esta lectura se escribió sobre una ejecución hecha con la máquina cargada** (otros procesos gastando una mediana de ~2 núcleos de 4; ver «Calidad de la medida»).
Se apoya en lo que es **robusto a la carga** —segundos de CPU de cada proceso, bytes, mensajes— y cita las latencias y las llamadas por segundo solo como orden de magnitud.
Una corrida con la máquina tranquila la debe confirmar o corregir.

**Dónde flux gana**

- **Enterarse (A).** El tiempo hasta enterarse pasa de «T/2 de media, T como máximo» a **milisegundos**: con T = 5 s, p50 de 2.5 s contra 3–176 ms (N = 1 … 1000); con T = 30 s, 15 s contra
  ~12 ms (N = 100). El sondeo solo baja esa espera pagando CPU: T = 1 s gasta **2.7×** la CPU de T = 5 s (8.7 % frente a 3.2 % de un núcleo con N = 100) y aún espera 0.5 s de media.
- **Y gasta menos CPU cuando lo que se vigila cambia poco.** Con N = 100 y T = 5 s el sondeo cuesta ~3.2 % de un núcleo **pase lo que pase** (no depende de C), y flux cuesta **0.50 %
  con 1 cambio/min** (6× menos), 1.7 % con 12/min y 6.2 % con 60/min. A C = 1 /min, además, el tráfico baja de 295 a 22 KiB/min (13× menos): casi todo el sondeo es preguntar por nada.
- **El coste del productor deja de depender de cuántos lo oyen (B).** Con N = 1000 receptores, el que publica gasta **1.4 ms de CPU por hecho** con flux y **72 ms** con webhooks (a 10 hechos/s,
  el 72 % de un núcleo solo en repartir). Con N = 100: 1.3 ms contra 11 ms. Es la ganancia real de «publicar un evento»: el productor ni sabe cuántos lo reciben, y no tiene cola de reintentos propia.

**Dónde no gana, o pierde**

- **Una llamada puntual por HTTP keep-alive sigue siendo más barata y más rápida (C).** Con el SDK tal como se publica, `request`/`respond` hace **5.6–19× menos llamadas por segundo** y gasta **6–21×
  más CPU por llamada** que `node:http` con keep-alive, según la concurrencia (19× y 21× a concurrencia 1, el extremo más sensible a la carga; 8× y 10× a 16; 5.6× y 6× a 256), con un p50 de 0.9–5.6 ms frente a 0.04–0.7 ms a
  concurrencia 1–16, y mueve ~5.5× más bytes por llamada (2.2 KB frente a 0.4 KB: el sobre CloudEvent completo en cada sentido). **Casi todo es del SDK, no del broker:** NATS «crudo» (core NATS, el mismo broker y
  permisos, sin sobre) iguala a HTTP keep-alive (1.3× su CPU a concurrencia 16 y 0.65× a 256; 3.5× a concurrencia 1, donde la ida y vuelta por el broker añade despertares), y
  una sola cosa del SDK —un `import()` que falla en cada llamada, **A-170**— explica por sí sola entre 2/3 y 5/6 del exceso: con `@opentelemetry/api` presente, flux queda en **~3.3×** la CPU de HTTP a concurrencia 16 y **1.7×** a 256
  (7.6× a 1). El resto es el sobre (identificadores, `time`, validaciones) y el doble recorrido por el broker. Para migrar solo por rendimiento una llamada que ya funciona sobre HTTP keep-alive, **no compensa** hoy.
- **Con muchos consumidores y cambios frecuentes, flux cuesta más que sondear.** Con N = 100 y T = 5 s el cruce está entre 12 y 60 cambios/min (a 600 cambios/min flux gasta **11×** la CPU del sondeo). La
  razón es estructural: el sondeo coalesce (si cambia tres veces entre dos sondeos, se entera una vez) y flux entrega, y confirma, **cada** cambio a **cada** durable. También **envía más bytes** en cuanto hay
  cambios (2.2× los del sondeo a 12 cambios/min con N ≥ 100, 8.8× a 60/min) y **usa más memoria**: con N = 1000, ~900 MB (CRM + clientes + broker) frente a ~320 MB (cada proceso de Node con el SDK carga ~20 MB más, y cada durable conectado cuesta ~0.2 MB en `nats-server`).
- **Con N pequeño el coste fijo de JetStream no se amortiza.** Con N = 1 el sondeo con ETag gasta 0.33 % de un núcleo y flux 0.41 %; con N = 10, empate (0.66 % y 0.59 %). Solo gana en CPU a partir de N ≈ 10–100 (y no siempre).
- **Avisar a muchos no ahorra CPU total, la mueve del productor al broker.** La CPU total por hecho de flux es **1.5× (N ≤ 10), 2.5× (N = 100) y 2.8× (N = 1000)** la de N webhooks keep-alive,
  y la entrega es más lenta (p50 de 12.8 ms frente a 8.8 ms con N = 100; 176 ms frente a 59 ms con N = 1000). Las dos cosas que flux da aquí **no están en esta tabla**: el productor no se bloquea ni reintenta,
  y los receptores caídos recuperan lo que se perdieron (durabilidad, backoff, DLQ), que un webhook «mínimo» no tiene.
- **El sondeo con `ETag` no ahorra nada aquí.** Con un recurso de ~120 B, el 304 cuesta lo mismo que el 200 (CPU y bytes casi iguales: 3.2 % y 455 KiB/min frente a 3.2 % y 461). El ahorro del `ETag` escala con el tamaño del
  cuerpo, que este banco no varía: es un límite de la medida.

**Por qué cuesta lo que cuesta (investigado)**

- **~2/3 del CPU de `nats-server` es escribir a disco el estado de cada consumidor** (`consumerFileStore.flushLoop → writeState → writeAtomically`: crear un fichero temporal y renombrarlo, por consumidor y por
  confirmación; perfil con `pprof`, **A-172**). Recreando los durables con `mem_storage: true` el mismo escenario gasta **0.23× (B) y 0.29× (A)** de CPU de `nats-server` y **0.36× y 0.51×** de CPU total
  (`bench/diagnostico/consumidores-en-memoria.mjs`, **A-173**): con ello el cruce de A se movería de ~25 a ~60 cambios/min (6.2 % × 0.51 ≈ 3.2 %, lo que cuesta el sondeo) y B bajaría de 2.7× a ~1–1.5× la CPU de los webhooks (N = 100, 10 hechos/s). **No es una recomendación**: `mem_storage` pierde el
  estado del consumidor si el servidor reinicia (reentrega desde el principio del stream). Es una decisión de diseño del protocolo, que para `state.watch` (foto + cambios, idempotente por diseño) podría tener sentido y para eventos no.
- **El coste por mensaje depende de la tasa**: con unos cientos de entregas/s a durables (B, N = 100, 2 hechos/s) `nats-server` gasta ~0.3 ms de CPU por entrega, y a ~20 000 mensajes/s de core NATS (C, NATS crudo) ~10–19 µs por llamada de dos mensajes.
  A tasas bajas domina el coste fijo de despertar y de escribir el estado del consumidor, no el de mover bytes.
- **Un `Authentication Timeout` sale como «credenciales rechazadas»** (PERMANENT): lo vio una repetición con la máquina a 19 de carga, y se reproduce sin carga (**A-174**).

**Cuándo no compensa migrar (según estas cifras)**

- Una llamada puntual entre dos servicios que ya funciona sobre HTTP keep-alive, **si el único motivo es el rendimiento**: flux no es más rápido ni más barato aquí. Se migra por el contrato (esquema, sobre, tenant, ACL por operación), no por velocidad.
- Pocos consumidores (N ≲ 10) de algo que cambia más de una vez cada pocos segundos, donde el sondeo con keep-alive es barato y simple.
- Fan-out a muchos receptores cuando lo único que se busca es **bajar CPU**: el total sube, aunque el productor se libere.

**Lo que esta ejecución no puede decir:** los porcentajes de CPU absolutos (con la máquina compartida, el planificador mete esperas y cambios de contexto que inflan todos los métodos), las latencias a concurrencia 1
(dependen de la carga que haya en ese instante: p50 de 0.04 ms para HTTP) ni el rendimiento máximo. Los **cocientes** entre métodos medidos a la vez (intercalados) son la parte fiable.
