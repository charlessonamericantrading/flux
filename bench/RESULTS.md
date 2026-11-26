# Resultados de `bench/`

> **Generado por `node bench/run.mjs`. No lo edites a mano:** cada número sale de los datos de la ejecución y se regenera con el mismo comando.
>
> **MEDIDO CON LA MÁQUINA CARGADA.** Durante la ejecución hubo otros procesos gastando CPU (mediana 1.93 núcleos ajenos, ver «Calidad de la medida»). Fíate de las cifras **robustas a la carga** (mensajes y peticiones emitidos, bytes en la red, segundos de CPU por proceso) y trata como **orientativas** las **sensibles** (latencias p50/p99/máx y llamadas por segundo): un vecino ocupado alarga las esperas y recorta el rendimiento sin que flux ni HTTP tengan nada que ver. Una corrida con la máquina tranquila las sustituye.

Ejecución: 2026-11-26T18:53:03.690Z · duración 83 min · 5 repetición(es) por punto · modo completo.

## Cómo leer las tablas

- Cada celda es **mediana [mín–máx]** de las repeticiones del punto. Las latencias son la mediana, entre repeticiones, del percentil de cada una (`n` = muestras sumadas).
- **Robustas a la carga de la máquina:** peticiones o mensajes emitidos, bytes en la red, segundos de CPU de cada proceso. **Sensibles** (marcadas con †): latencias y llamadas por segundo; con la máquina cargada son orientativas.
- CPU: `/proc/<pid>` de cada proceso desde fuera (clientes = suma de todos los procesos de consumidores; servidor = el CRM o el productor; `nats-server`). «% de un núcleo» es CPU / tiempo de ventana: 100 % = un núcleo entero.
- Bytes: de APLICACIÓN en los sockets (HTTP: `bytesRead + bytesWritten` del lado servidor; NATS: `in_bytes + out_bytes` de `/varz`). Sin cabeceras TCP/IP y, con TLS, ya descifrados (ni `/varz` ni el `TLSSocket` de Node cuentan los registros TLS: el coste en bytes de TLS no está en estas tablas).
- Peticiones o mensajes emitidos: HTTP = peticiones recibidas por el servidor; flux = `in_msgs` de `nats-server` (todo lo que los clientes le mandan: publicaciones, peticiones de mensajes, acuses…).
- **Estado**: `✓` sin incidencias; `⚠n` = n repeticiones con alguna comprobación del banco fallida; `~n` = n repeticiones con avisos de fiabilidad (proceso saturado, carga ajena); `✗n` = n repeticiones que fallaron.

## Lectura

### Cruces calculados con los datos

**A. Enterarse de un cambio** (C = 12 cambios/min, T = 5 s; flux frente al sondeo con ETag; `×` = flux / sondeo: menos de 1 es que flux gasta menos):

| N | CPU total | Bytes | Mensajes emitidos | Tiempo hasta enterarse (p50) |
|---|---:|---:|---:|---:|
| 1 | 1.2× | 6.9× | 3.7× | 1/802× |
| 10 | 0.90× | 2.7× | 1.4× | 1/524× |
| 100 | 0.52× | 2.2× | 1.2× | 1/206× |
| 1000 | 0.79× | 2.2× | 1.3× | 1/14× |

**B. Avisar a muchos** (H = 120 hechos/min; flux frente a N webhooks; `×` = flux / webhooks):

| N | CPU total por hecho | CPU del productor por hecho | Bytes por hecho | Entrega (p50) |
|---|---:|---:|---:|---:|
| 1 | 1.5× | 0.61× | 2.4× | 2.2× |
| 10 | 1.5× | 0.31× | 1.3× | 1.8× |
| 100 | 2.5× | 0.11× | 1.2× | 1.5× |
| 1000 | 2.8× | 1/51× | 1.2× | 3.0× |

**C. Llamada con respuesta** (todo frente a HTTP con keep-alive; `×` mayor que 1 en latencia o CPU es PEOR que HTTP):

| Concurrencia | HTTP rinde (llamadas/s) ×  más que flux | Latencia p50 de flux × | CPU /llamada de flux × | CPU /llamada de NATS crudo × | CPU /llamada de flux con OTel × |
|---|---:|---:|---:|---:|---:|
| 1 | 19× | 25× | 21× | 3.5× | 7.6× |
| 16 | 8.3× | 8.2× | 9.6× | 1.3× | 3.3× |
| 256 | 5.6× | 5.6× | 6.0× | 0.65× | 1.7× |

### Lectura escrita a mano

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

**Una comprobación del propio banco falla en 5 de 90 repeticiones de C, todas «flux a concurrencia 256»:** las llamadas que cuenta el cliente y las que cuenta el responder difieren un 10–17 %, en las dos direcciones (el responder cuenta de menos y de más), con la máquina a 10–22 de carga. Es el **desfase entre las dos lecturas** (se toman por IPC en dos procesos y con la carga tardan lo que tarden: a 4 000 llamadas/s, 0.6 s son 2 400 llamadas), no una pérdida (una pérdida iría siempre en el mismo sentido y `in_msgs` de `nats-server` cuadra en las 55). Esas cinco repeticiones tienen, por tanto, una incertidumbre de ese orden en «CPU por llamada» y «llamadas/s» a concurrencia 256; el resto no la tiene. Con la máquina en calma debe desaparecer: si no, es un fallo del banco.

## Escenario A — enterarse de un cambio

N consumidores quieren saber cuanto antes que un cliente cambió; el CRM lo cambia C veces por minuto. Línea base: cada consumidor sondea `GET /clientes/cli-1` cada T segundos con keep-alive (`node:http`, un `Agent` y una conexión por consumidor), «ingenuo» (siempre 200 con el cuerpo) o con `If-None-Match` (304). flux: cada consumidor es un servicio con su `bus.state.watch`. El centro de los barridos es **N = 100, C = 12 /min, T = 5 s**; cada tabla varía un factor.

#### A.1 — Barrido de N (C = 12 /min, T = 5 s)

| N | Método | Peticiones o mensajes emitidos /min | CPU total (% de un núcleo) | · clientes | · servidor | · nats-server | KiB en la red /min | Enterarse p50 † | p99 † | máx † | n | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | HTTP sondeo con ETag | 11.4 [11.4–14.3] | 0.330 [0.292–0.376] | 0.183 [0.102–0.197] | 0.168 [0.116–0.192] | 0 | 4.57 [4.13–5.27] | 2.48 s [896 ms–2.81 s] | 4.56 s [3.88 s–4.95 s] | 4.56 s [3.88 s–4.95 s] | 19 | ~5 |
|  | flux `state.watch` | 42.8 [34.3–42.8] | 0.411 [0.351–0.466] | 0.025 [0.024–0.027] | 0.056 [0.053–0.062] | 0.334 [0.272–0.378] | 31.7 [25.3–31.7] | 3.09 ms [2.96 ms–3.52 ms] | 3.89 ms [3.51 ms–4.72 ms] | 3.89 ms [3.51 ms–4.72 ms] | 23 | ~4 |
| 10 | HTTP sondeo con ETag | 120 [114–123] | 0.661 [0.548–0.690] | 0.359 [0.257–0.386] | 0.304 [0.291–0.312] | 0 | 44.9 [44.4–47.4] | 2.49 s [2.47 s–3.20 s] | 4.92 s [4.53 s–4.95 s] | 4.92 s [4.53 s–4.95 s] | 190 | ~5 |
|  | flux `state.watch` | 171 [137–171] | 0.594 [0.542–0.668] | 0.139 [0.111–0.151] | 0.064 [0.060–0.065] | 0.383 [0.351–0.465] | 119 [95.3–119] | 4.75 ms [3.83 ms–5.28 ms] | 10.3 ms [6.01 ms–14.4 ms] | 10.3 ms [6.01 ms–14.4 ms] | 230 | ~4 |
| 100 | HTTP sondeo ingenuo | 1199 [1190–1216] | 3.17 [3.06–3.24] | 1.96 [1.89–2.02] | 1.21 [1.16–1.26] | 0 | 455 [452–462] | 2.54 s [2.45 s–2.68 s] | 4.97 s [4.95 s–4.98 s] | 5.00 s [4.99 s–5.00 s] | 1900 | ~5 |
|  | HTTP sondeo con ETag | 1199 [1190–1216] | 3.23 [3.05–3.30] | 1.96 [1.85–2.03] | 1.24 [1.20–1.29] | 0 | 461 [449–467] | 2.54 s [2.43 s–2.68 s] | 4.97 s [4.91 s–4.98 s] | 4.99 s [4.99 s–5.00 s] | 1900 | ~4 |
|  | flux `state.watch` | 1456 [1164–1527] | 1.69 [1.48–1.84] | 0.392 [0.318–0.487] | 0.061 [0.055–0.063] | 1.18 [1.04–1.29] | 999 [796–1010] | 12.4 ms [10.5 ms–15.7 ms] | 25.8 ms [20.7 ms–38.8 ms] | 26.6 ms [21.1 ms–39.7 ms] | 2300 | ~5 |
| 1000 | HTTP sondeo ingenuo | 12016 [11977–12065] | 18.2 [17.0–19.2] | 13.6 [12.9–13.9] | 4.39 [3.97–5.61] | 0 | 4564 [4550–4583] | 2.50 s [2.46 s–2.54 s] | 4.94 s [4.94 s–4.96 s] | 5.00 s [5.00 s–5.00 s] | 19000 | ~5 |
|  | HTTP sondeo con ETag | 12015 [11980–12067] | 18.5 [17.2–19.3] | 13.4 [12.9–14.1] | 4.88 [4.30–5.58] | 0 | 4596 [4502–4657] | 2.50 s [2.47 s–2.54 s] | 4.95 s [4.94 s–4.96 s] | 5.00 s [5.00 s–5.00 s] | 19000 | ~5 |
|  | flux `state.watch` | 15468 [12568–16133] | 14.5 [12.2–16.4] | 5.89 [4.94–6.64] | 0.055 [0.047–0.068] | 8.47 [7.17–9.68] | 10006 [7975–10074] | 176 ms [103 ms–186 ms] | 330 ms [193 ms–334 ms] | 344 ms [207 ms–394 ms] | 23000 | ~5 |

#### A.2 — Barrido de C (N = 100, T = 5 s)

| C (cambios /min) | Método | Peticiones o mensajes emitidos /min | CPU total (% de un núcleo) | · clientes | · servidor | · nats-server | KiB en la red /min | Enterarse p50 † | p99 † | máx † | n | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | HTTP sondeo con ETag | 1199 [1190–1216] | 3.17 [2.85–3.38] | 1.95 [1.75–2.03] | 1.22 [1.10–1.40] | 0 | 295 [293–299] | — | — | — | 0 | ~4 |
|  | flux `state.watch` | 0 [0–71.4] | 0.501 [0.486–0.579] | 0.101 [0.087–0.137] | 0.003 [0.003–0.004] | 0.403 [0.381–0.437] | 22.0 [20.9–33.2] | — | — | — | 0 | ~4 |
| 12 | HTTP sondeo ingenuo | 1199 [1190–1216] | 3.17 [3.06–3.24] | 1.96 [1.89–2.02] | 1.21 [1.16–1.26] | 0 | 455 [452–462] | 2.54 s [2.45 s–2.68 s] | 4.97 s [4.95 s–4.98 s] | 5.00 s [4.99 s–5.00 s] | 1900 | ~5 |
|  | HTTP sondeo con ETag | 1199 [1190–1216] | 3.23 [3.05–3.30] | 1.96 [1.85–2.03] | 1.24 [1.20–1.29] | 0 | 461 [449–467] | 2.54 s [2.43 s–2.68 s] | 4.97 s [4.91 s–4.98 s] | 4.99 s [4.99 s–5.00 s] | 1900 | ~4 |
|  | flux `state.watch` | 1456 [1164–1527] | 1.69 [1.48–1.84] | 0.392 [0.318–0.487] | 0.061 [0.055–0.063] | 1.18 [1.04–1.29] | 999 [796–1010] | 12.4 ms [10.5 ms–15.7 ms] | 25.8 ms [20.7 ms–38.8 ms] | 26.6 ms [21.1 ms–39.7 ms] | 2300 | ~5 |
| 60 | HTTP sondeo con ETag | 1199 [1190–1216] | 3.25 [2.97–3.31] | 1.99 [1.78–2.01] | 1.26 [1.19–1.31] | 0 | 482 [479–489] | 2.54 s [2.51 s–2.56 s] | 4.95 s [4.92 s–4.97 s] | 5.00 s [5.00 s–5.01 s] | 7600 | ~5 |
|  | flux `state.watch` | 6462 [5603–6755] | 6.20 [5.61–6.95] | 1.67 [1.53–1.70] | 0.249 [0.213–0.263] | 4.23 [3.80–4.99] | 4226 [3806–4424] | 12.3 ms [10.7 ms–15.4 ms] | 35.8 ms [22.5 ms–40.7 ms] | 38.0 ms [23.5 ms–42.3 ms] | 9700 | ~5 |
| 600 | HTTP sondeo con ETag | 1199 [1190–1216] | 3.34 [3.33–3.56] | 1.95 [1.94–2.13] | 1.40 [1.38–1.46] | 0 | 485 [482–492] | 2.51 s [2.50 s–2.51 s] | 4.95 s [4.95 s–4.95 s] | 5.00 s [5.00 s–5.00 s] | 76200 | ~4 |
|  | flux `state.watch` | 62038 [59519–63201] | 36.1 [33.3–40.6] | 6.52 [5.76–6.83] | 1.89 [1.77–1.99] | 27.7 [25.8–31.8] | 41183 [39392–42046] | 6.13 ms [5.68 ms–7.44 ms] | 16.1 ms [12.9 ms–24.3 ms] | 37.1 ms [16.8 ms–72.7 ms] | 94900 | ~3 |

#### A.3 — Barrido de T (N = 100, C = 12 /min; flux no sondea y no depende de T)

| T (s) | Método | Peticiones o mensajes emitidos /min | CPU total (% de un núcleo) | · clientes | · servidor | · nats-server | KiB en la red /min | Enterarse p50 † | p99 † | máx † | n | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | HTTP sondeo con ETag | 5999 [5997–6006] | 8.71 [8.39–9.31] | 5.39 [4.98–5.64] | 3.41 [3.29–3.67] | 0 | 1694 [1693–1750] | 499 ms [495 ms–502 ms] | 985 ms [968 ms–993 ms] | 999 ms [987 ms–1.00 s] | 1900 | ~5 |
| 5 | HTTP sondeo ingenuo | 1199 [1190–1216] | 3.17 [3.06–3.24] | 1.96 [1.89–2.02] | 1.21 [1.16–1.26] | 0 | 455 [452–462] | 2.54 s [2.45 s–2.68 s] | 4.97 s [4.95 s–4.98 s] | 5.00 s [4.99 s–5.00 s] | 1900 | ~5 |
|  | HTTP sondeo con ETag | 1199 [1190–1216] | 3.23 [3.05–3.30] | 1.96 [1.85–2.03] | 1.24 [1.20–1.29] | 0 | 461 [449–467] | 2.54 s [2.43 s–2.68 s] | 4.97 s [4.91 s–4.98 s] | 4.99 s [4.99 s–5.00 s] | 1900 | ~4 |
| 30 | HTTP sondeo con ETag | 201 [195–204] | 0.655 [0.626–0.673] | 0.395 [0.374–0.400] | 0.265 [0.252–0.274] | 0 | 80.0 [78.0–81.4] | 14.9 s [14.7 s–15.4 s] | 29.7 s [29.6 s–29.8 s] | 30.0 s [29.9 s–30.0 s] | 4100 | ~5 |

#### A.4 — Con TLS (N = 100, C = 12 /min, T = 5 s)

| N | Método | Peticiones o mensajes emitidos /min | CPU total (% de un núcleo) | · clientes | · servidor | · nats-server | KiB en la red /min | Enterarse p50 † | p99 † | máx † | n | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | HTTP sondeo con ETag | 1199 [1190–1216] | 3.48 [3.39–3.73] | 2.04 [1.97–2.22] | 1.44 [1.42–1.57] | 0 | 460 [449–467] | 2.55 s [2.45 s–2.68 s] | 4.97 s [4.95 s–4.98 s] | 5.00 s [4.99 s–5.00 s] | 1900 | ~4 |
|  | flux `state.watch` | 1456 [1165–1456] | 1.84 [1.57–1.94] | 0.514 [0.448–0.563] | 0.064 [0.059–0.066] | 1.26 [1.04–1.36] | 996 [796–996] | 14.7 ms [12.4 ms–18.9 ms] | 32.5 ms [25.1 ms–36.7 ms] | 33.1 ms [26.1 ms–50.0 ms] | 2300 | ~5 |

## Escenario B — avisar a muchos

Un productor, N receptores, un hecho (pedido creado) cada 60/H segundos. Línea base: N webhooks por hecho (un `POST` por receptor, keep-alive, hasta 3 intentos, sin cola). flux: 1 evento publicado y N consumidores durables (un servicio cada uno). Latencia = del instante en que el productor decide avisar al instante en que el receptor tiene el hecho.

#### B.1 — Barrido de N (H = 120 hechos/min)

| N | Método | Peticiones o mensajes emitidos /hecho | CPU total (ms /hecho) | · productor | · receptores (suma) | · nats-server | KiB en la red /hecho | Entrega p50 † | p99 † | máx † | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | HTTP webhooks (N POST) | 1.00 | 4.15 [3.66–4.80] | 2.62 [2.19–2.88] | 1.60 [1.32–2.18] | 0 | 0.683 | 0.87 ms [0.85 ms–0.95 ms] | 1.61 ms [1.31 ms–3.23 ms] | 1.61 ms [1.31 ms–3.23 ms] | ~3 |
|  | flux 1 evento + N durables | 2.04 [2.04–2.05] | 6.39 [5.33–7.37] | 1.60 [1.38–1.75] | 1.06 [1.01–1.22] | 3.74 [2.94–4.41] | 1.61 [1.61–1.61] | 1.91 ms [1.73 ms–1.96 ms] | 3.45 ms [2.38 ms–5.13 ms] | 3.45 ms [2.38 ms–5.13 ms] | ~2 |
| 10 | HTTP webhooks (N POST) | 10.0 | 7.67 [6.85–8.58] | 4.70 [4.03–5.06] | 3.01 [2.51–3.51] | 0 | 6.83 | 1.91 ms [1.59 ms–2.37 ms] | 4.24 ms [3.23 ms–9.26 ms] | 4.43 ms [3.48 ms–9.45 ms] | ~5 |
|  | flux 1 evento + N durables | 11.4 [11.4–11.5] | 11.6 [10.8–14.3] | 1.45 [1.35–1.59] | 3.60 [3.31–3.88] | 6.69 [5.80–8.86] | 8.73 [8.73–8.73] | 3.41 ms [2.85 ms–5.09 ms] | 13.0 ms [4.49 ms–16.6 ms] | 13.8 ms [4.71 ms–16.7 ms] | ~4 |
| 100 | HTTP webhooks (N POST) | 100 | 18.4 [17.6–19.6] | 11.0 [10.5–12.2] | 7.38 [7.12–7.42] | 0 | 68.4 | 8.77 ms [7.86 ms–9.41 ms] | 68.3 ms [32.5 ms–77.1 ms] | 69.2 ms [33.0 ms–97.1 ms] | ~5 |
|  | flux 1 evento + N durables | 105 [105–106] | 45.6 [41.5–52.4] | 1.25 [1.19–1.40] | 12.4 [12.0–13.7] | 32.3 [28.3–37.3] | 79.9 [79.9–80.0] | 12.8 ms [12.3 ms–19.0 ms] | 55.2 ms [36.8 ms–77.2 ms] | 65.6 ms [39.2 ms–81.0 ms] | ~5 |
| 1000 | HTTP webhooks (N POST) | 1000 | 138 [134–149] | 72.0 [68.7–77.4] | 65.6 [65.2–72.0] | 0 | 684 | 59.4 ms [47.1 ms–69.0 ms] | 175 ms [134 ms–257 ms] | 236 ms [151 ms–279 ms] | ~5 |
|  | flux 1 evento + N durables | 1083 [1006–1096] | 383 [359–393] | 1.42 [1.37–1.59] | 87.5 [83.4–95.2] | 293 [275–299] | 816 [764–826] | 176 ms [154 ms–206 ms] | 380 ms [331 ms–489 ms] | 461 ms [362 ms–543 ms] | ✗1 ~4 |

#### B.2 — Barrido de H (N = 100)

| H (hechos /min) | Método | Peticiones o mensajes emitidos /hecho | CPU total (ms /hecho) | · productor | · receptores (suma) | · nats-server | KiB en la red /hecho | Entrega p50 † | p99 † | máx † | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 120 | HTTP webhooks (N POST) | 100 | 18.4 [17.6–19.6] | 11.0 [10.5–12.2] | 7.38 [7.12–7.42] | 0 | 68.4 | 8.77 ms [7.86 ms–9.41 ms] | 68.3 ms [32.5 ms–77.1 ms] | 69.2 ms [33.0 ms–97.1 ms] | ~5 |
|  | flux 1 evento + N durables | 105 [105–106] | 45.6 [41.5–52.4] | 1.25 [1.19–1.40] | 12.4 [12.0–13.7] | 32.3 [28.3–37.3] | 79.9 [79.9–80.0] | 12.8 ms [12.3 ms–19.0 ms] | 55.2 ms [36.8 ms–77.2 ms] | 65.6 ms [39.2 ms–81.0 ms] | ~5 |
| 600 | HTTP webhooks (N POST) | 100 | 10.3 [9.97–10.6] | 6.34 [6.10–6.53] | 3.92 [3.88–4.05] | 0 | 68.4 | 4.07 ms [3.89 ms–4.69 ms] | 26.2 ms [15.0 ms–61.0 ms] | 33.7 ms [32.1 ms–69.6 ms] | ~5 |
|  | flux 1 evento + N durables | 105 [105–106] | 28.0 [26.6–29.4] | 1.29 [1.26–1.31] | 5.84 [5.54–6.16] | 20.9 [19.7–22.4] | 80.0 [79.9–80.0] | 7.07 ms [6.46 ms–7.88 ms] | 62.5 ms [29.7 ms–83.9 ms] | 68.4 ms [46.9 ms–117 ms] | ~5 |

#### B.3 — Con TLS (N = 100, H = 120 /min)

| N | Método | Peticiones o mensajes emitidos /hecho | CPU total (ms /hecho) | · productor | · receptores (suma) | · nats-server | KiB en la red /hecho | Entrega p50 † | p99 † | máx † | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | HTTP webhooks (N POST) | 100 | 24.7 [24.3–30.7] | 14.5 [14.1–18.4] | 10.1 [9.85–12.7] | 0 | 68.4 | 10.9 ms [9.93 ms–11.9 ms] | 42.2 ms [25.0 ms–61.4 ms] | 42.8 ms [26.0 ms–62.3 ms] | ~5 |
|  | flux 1 evento + N durables | 105 [105–106] | 57.6 [49.5–65.1] | 1.54 [1.43–1.70] | 18.6 [18.2–23.7] | 37.6 [29.7–42.8] | 79.9 [79.9–80.0] | 18.7 ms [16.1 ms–22.8 ms] | 49.8 ms [34.0 ms–61.5 ms] | 51.5 ms [36.0 ms–66.6 ms] | ~5 |

## Escenario C — llamada con respuesta

`calcular-descuento`: petición de ~60 B y respuesta de ~35 B de JSON, en bucle cerrado a concurrencia 1, 16 y 256 (cada hilo de ejecución lanza la siguiente al recibir la respuesta). Un proceso de Node para cada lado: a concurrencia ≥ 16 el proceso más lento llega a un núcleo, así que «llamadas/s» mide el techo de ese par de procesos, no de HTTP ni de NATS en abstracto; la **CPU por llamada** es lo que sigue siendo comparable. «NATS crudo» es core NATS sin el SDK de flux (el mismo broker y permisos): reparte el coste entre el transporte y lo que añade el SDK. «flux con OpenTelemetry presente» es el mismo SDK con `@opentelemetry/api` resolviendo (experimento: ver «Hallazgos del banco»).

#### C.1 — Sin TLS

| Concurrencia | Método | Llamadas /s † | p50 † | p99 † | máx † | CPU total (µs /llamada) | · cliente | · servidor | · nats-server | Bytes /llamada | Conexiones TCP nuevas /llamada | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | HTTP keep-alive | 17244 [11081–21854] | 0.04 ms [0.03 ms–0.07 ms] | 0.28 ms [0.20 ms–0.40 ms] | 17.3 ms [6.17 ms–72.2 ms] | 53.5 [44.4–90.1] | 32.5 [27.8–53.8] | 21.0 [16.6–36.3] | 0 | 395 [395–395] | 0.000 | ~5 |
|  | HTTP sin keep-alive | 5139 [3931–5597] | 0.14 ms [0.12 ms–0.20 ms] | 0.73 ms [0.71 ms–0.82 ms] | 23.8 ms [21.6 ms–63.1 ms] | 192 [171–289] | 118 [103–175] | 73.9 [67.6–114] | 0 | 360 | 1.000 [1.000–1.000] | ~5 |
|  | NATS crudo (sin SDK) | 6056 [5241–7142] | 0.14 ms [0.12 ms–0.16 ms] | 0.57 ms [0.44 ms–0.62 ms] | 37.9 ms [9.26 ms–48.8 ms] | 187 [154–213] | 70.3 [58.6–76.0] | 35.2 [29.2–39.2] | 81.1 [65.5–98.3] | 182 [182–182] | 0.000 | ~5 |
|  | flux `request`/`respond` | 893 [848–970] | 0.95 ms [0.90 ms–1.06 ms] | 4.45 ms [2.87 ms–5.95 ms] | 41.3 ms [20.6 ms–105 ms] | 1139 [1076–1236] | 463 [457–506] | 425 [415–464] | 230 [203–277] | 2172 [2172–2172] | 0.000 | ~5 |
|  | flux (con OpenTelemetry presente, experimento) | 2634 [2580–2792] | 0.30 ms [0.29 ms–0.33 ms] | 1.38 ms [1.09 ms–1.51 ms] | 26.9 ms [6.25 ms–34.9 ms] | 405 [397–470] | 160 [152–167] | 128 [119–135] | 132 [108–168] | 2172 [2172–2173] | 0.000 | ~5 |
| 16 | HTTP keep-alive | 19859 [15937–25900] | 0.69 ms [0.58 ms–0.80 ms] | 2.31 ms [1.74 ms–3.23 ms] | 24.5 ms [5.76 ms–70.7 ms] | 51.6 [49.2–62.0] | 31.7 [30.2–37.2] | 19.9 [18.7–24.8] | 0 | 395 [395–395] | 0.000 | ~5 |
|  | HTTP sin keep-alive | 8856 [6303–11861] | 1.60 ms [1.17 ms–2.11 ms] | 5.09 ms [3.55 ms–10.8 ms] | 28.7 ms [16.7 ms–55.6 ms] | 134 [111–149] | 84.6 [70.8–94.0] | 48.7 [40.4–55.3] | 0 | 360 [360–360] | 1.000 [1.000–1.000] | ~5 |
|  | NATS crudo (sin SDK) | 18113 [16483–19675] | 0.66 ms [0.56 ms–0.84 ms] | 5.94 ms [4.64 ms–6.77 ms] | 26.7 ms [11.9 ms–76.3 ms] | 64.6 [52.4–70.8] | 33.4 [29.2–37.0] | 10.6 [8.62–12.5] | 18.8 [14.1–21.4] | 182 [182–182] | 0.000 | ~5 |
|  | flux `request`/`respond` | 2399 [2267–3614] | 5.61 ms [3.98 ms–6.42 ms] | 21.8 ms [10.9 ms–23.5 ms] | 40.9 ms [30.7 ms–78.1 ms] | 497 [450–526] | 217 [195–231] | 198 [176–211] | 82.9 [67.3–91.4] | 2172 [2171–2174] | 0.000 | ~5 |
|  | flux (con OpenTelemetry presente, experimento) | 6924 [6050–12886] | 1.84 ms [1.07 ms–2.14 ms] | 8.74 ms [3.94 ms–11.0 ms] | 26.6 ms [12.5 ms–55.9 ms] | 169 [157–179] | 71.1 [69.1–79.6] | 49.6 [47.2–54.9] | 44.2 [40.3–49.7] | 2172 [2172–2172] | 0.000 | ~5 |
| 256 | HTTP keep-alive | 20034 [12436–21207] | 11.7 ms [11.2 ms–17.3 ms] | 31.8 ms [24.4 ms–66.5 ms] | 51.2 ms [44.1 ms–107 ms] | 67.4 [59.7–75.6] | 43.0 [38.0–47.8] | 24.5 [21.7–27.8] | 0 | 395 [395–395] | 0.000 | ~5 |
|  | HTTP sin keep-alive | 6710 [4118–8633] | 35.9 ms [28.2 ms–47.3 ms] | 79.6 ms [58.9 ms–334 ms] | 97.8 ms [73.8 ms–495 ms] | 160 [156–205] | 109 [105–137] | 51.2 [50.5–68.2] | 0 | 360 [360–362] | 1.002 [1.000–1.006] | ~5 |
|  | NATS crudo (sin SDK) | 25542 [22967–25664] | 8.51 ms [8.00 ms–9.44 ms] | 27.6 ms [24.4 ms–30.3 ms] | 90.0 ms [35.7 ms–104 ms] | 43.7 [40.4–47.3] | 28.3 [26.0–30.1] | 5.68 [5.21–6.27] | 9.78 [9.00–11.0] | 182 [182–182] | 0.000 | ~5 |
|  | flux `request`/`respond` | 3605 [2282–4600] | 64.9 ms [51.9 ms–94.8 ms] | 196 ms [91.7 ms–475 ms] | 231 ms [103 ms–535 ms] | 402 [370–470] | 175 [162–201] | 161 [155–199] | 66.3 [53.4–69.5] | 2169 [1981–2211] | 0.000 | ⚠3 ~5 |
|  | flux (con OpenTelemetry presente, experimento) | 12275 [9759–15129] | 19.5 ms [16.0 ms–23.9 ms] | 54.3 ms [35.3 ms–92.0 ms] | 83.1 ms [51.9 ms–162 ms] | 117 [112–126] | 55.2 [52.9–62.0] | 35.8 [33.2–37.7] | 27.2 [23.7–28.2] | 2159 [2107–2180] | 0.000 | ⚠2 ~5 |

#### C.2 — Con TLS (concurrencia 16)

| Concurrencia | Método | Llamadas /s † | p50 † | p99 † | máx † | CPU total (µs /llamada) | · cliente | · servidor | · nats-server | Bytes /llamada | Conexiones TCP nuevas /llamada | Estado |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 16 | HTTP keep-alive | 13046 [11743–13734] | 1.05 ms [1.00 ms–1.16 ms] | 3.60 ms [3.31 ms–4.64 ms] | 38.0 ms [25.3 ms–48.0 ms] | 96.0 [82.8–98.8] | 61.1 [52.9–62.2] | 34.9 [29.9–36.7] | 0 | 395 [395–395] | 0.000 | ~5 |
|  | NATS crudo (sin SDK) | 14465 [13071–18239] | 0.90 ms [0.69 ms–0.98 ms] | 6.07 ms [4.84 ms–6.52 ms] | 24.1 ms [12.9 ms–47.8 ms] | 99.2 [94.2–123] | 42.8 [41.9–52.3] | 22.2 [20.8–27.5] | 34.1 [30.5–42.9] | 182 [182–182] | 0.000 | ~5 |
|  | flux `request`/`respond` | 2391 [2023–3204] | 5.75 ms [4.63 ms–6.47 ms] | 22.0 ms [10.7 ms–30.8 ms] | 86.5 ms [26.9 ms–171 ms] | 567 [540–645] | 263 [249–293] | 225 [214–254] | 85.1 [75.6–97.4] | 2172 [2171–2174] | 0.000 | ~5 |

## Hallazgos del banco

Cosas que el banco descubrió y que no son una cifra de la malla. Cada una está, con su medida, en el Apéndice A de `docs/plan-v1.1/PLAN.md`, y las que se reproducen con un script lo traen en `bench/diagnostico/`. **No se ha tocado ningún `sdk-*/src`.**

- **A-170 — `activeTraceparent()` hace un `import()` de `@opentelemetry/api` en CADA publicación, petición y respuesta** (`sdk-node/src/context.ts`). Sin el paquete instalado, cada llamada recorre la resolución de módulos de Node y lanza una excepción que se descarta: ~85 µs de CPU por llamada, en las dos puntas de una petición; con el paquete resuelto, ~3 µs. Es la diferencia entre «flux» y «flux (con OpenTelemetry presente)» en C.
- **A-171 — `gen()` de `scripts/lib/nats-harness.mjs` se rompe con ~1 000 servicios:** `spawnSync` corta la salida de `gen-acl.mjs` en 1 MiB (~1.06 KB por servicio) y devuelve `status: null`; `dev-secure.mjs` fallaría con «gen-acl.mjs falló» sin motivo. El banco usa su propio `execFile` con `maxBuffer` alto.
- **A-172 — Casi 2/3 de la CPU de `nats-server` en B (y en A) es escribir el estado de cada consumidor durable a disco** (`consumerFileStore.flushLoop → writeState → writeAtomically`: fichero temporal + renombrado, por consumidor), no mover mensajes. Perfil con `pprof` (`prof_port`).
- **A-173 — Con los durables en memoria (`mem_storage: true`) la CPU de `nats-server` baja a 0.23× (B) y 0.29× (A), y la total a 0.36× y 0.51×:** `node bench/diagnostico/consumidores-en-memoria.mjs [a|b]`. No es una recomendación (el estado del consumidor se pierde al reiniciar el servidor): es lo que cuesta no hacerlo, para que el dueño del protocolo decida.
- **A-174 — Un `Authentication Timeout` del broker sale como «credenciales rechazadas» (`ConnectionRejectedError`, PERMANENT):** `permanentConnectError` (`sdk-node/src/security.ts`) trata todo `AuthorizationError` igual. Pasó una vez con la máquina a 19 de carga (N = 1000) y se reproduce sin carga: `node bench/diagnostico/auth-timeout.mjs`. Un servicio que arranca con el broker saturado no reintenta: se cae.

## Dónde se gasta la CPU

Fracción de la CPU de cada proceso que se gasta **en el núcleo del sistema operativo** (red, `epoll`, `futex`, ficheros) y no en código de usuario: mediana de todas las repeticiones sin TLS de cada método. Un proceso que casi todo lo gasta en el núcleo está pagando entregas, despertares y escrituras más que cálculo.

| Escenario | Método | clientes | servidor | nats-server |
|---|---|---:|---:|---:|
| A | HTTP sondeo con ETag | 11 % | 15 % | — |
| A | flux `state.watch` | 11 % | 0 % | 63 % |
| A | HTTP sondeo ingenuo | 12 % | 23 % | — |
| B | HTTP webhooks (N POST) | 14 % | 15 % | — |
| B | flux 1 evento + N durables | 16 % | 17 % | 65 % |
| C | HTTP keep-alive | 17 % | 22 % | — |
| C | HTTP sin keep-alive | 31 % | 29 % | — |
| C | NATS crudo (sin SDK) | 6 % | 11 % | 29 % |
| C | flux `request`/`respond` | 14 % | 17 % | 49 % |
| C | flux (con OpenTelemetry presente, experimento) | 7 % | 12 % | 35 % |

## Calidad de la medida

|  |  |
|---|---:|
| Repeticiones medidas (sin contar las que fallaron) | 249 de 250 |
| Repeticiones repetidas por aviso de fiabilidad | 0 |
| Carga media (1 min) al empezar cada repetición: mediana / máx | 7.62 / 30.3 |
| Carga media (1 min) al terminar cada repetición: mediana / máx | 7.62 / 30.3 |
| CPU gastada por procesos AJENOS al banco (núcleos): mediana / p95 / máx | 1.93 / 3.85 / 3.93 |
| Repeticiones con carga ajena > 0.25 núcleos | 233 de 249 |
| Repeticiones con la máquina casi llena (> 85 % de las CPU) | 97 de 249 |
| Repeticiones con algún proceso del banco saturado (> 85 % de un núcleo) | 9 de 249 |
| Procesos ajenos más frecuentes entre los ocupados | `claude` (207), `dotnet` (158), `bash` (106), `java` (69), `pinner.sh` (37), `node` (36), `go` (24), `nats-server` (20) |

## Comprobaciones del propio banco

Antes de fiarse de un número hay que saber que mide lo que se cree. Cada repetición se comprueba contra sus propios contadores (keep-alive de verdad contando conexiones TCP, `304` en el sondeo con ETag, cada mensaje entregado una vez, ningún proceso saturado). `bench/autoprueba.mjs` estropea el banco a propósito y exige que estas comprobaciones lo detecten.

| Comprobación | Repeticiones que la cumplen |  |
|---|---:|---:|
| A: 200 + 304 = peticiones | 60 / 60 | ✓ |
| A: cada observador recibe cada cambio exactamente una vez | 40 / 40 | ✓ |
| A: con ETag, los sondeos sin cambio reciben 304 (y 200 solo por cambios) | 50 / 50 | ✓ |
| A: el sondeo ingenuo nunca recibe 304 | 10 / 10 | ✓ |
| A: keep-alive real: ninguna conexión nueva en la ventana | 60 / 60 | ✓ |
| A: lo emitido por los clientes es lo recibido por el servidor (±N en vuelo) | 60 / 60 | ✓ |
| A: ningún cambio sin ver | 40 / 40 | ✓ |
| A: sin avisos de fiabilidad (proceso saturado, carga ajena) | 9 / 100 | ⚠ |
| A: sin errores ni sondeos omitidos | 60 / 60 | ✓ |
| A: todos los puts se aplicaron | 40 / 40 | ✓ |
| A: una conexión viva por consumidor | 60 / 60 | ✓ |
| B: cada evento se entrega una vez a cada consumidor (sin duplicados) | 29 / 29 | ✓ |
| B: cada receptor recibe cada hecho (ninguno perdido) | 59 / 59 | ✓ |
| B: el productor no tuvo errores de publicación | 29 / 29 | ✓ |
| B: keep-alive real: ninguna conexión nueva en la ventana | 30 / 30 | ✓ |
| B: ninguna entrega ajena a la corrida | 59 / 59 | ✓ |
| B: ningún cuerpo inválido ni duplicado | 30 / 30 | ✓ |
| B: sin avisos de fiabilidad (proceso saturado, carga ajena) | 6 / 59 | ⚠ |
| B: un POST por hecho y receptor (±N en vuelo), sin reintentos ni fallos | 30 / 30 | ✓ |
| B: una conexión viva por receptor | 30 / 30 | ✓ |
| C: el broker vio ~2 mensajes por llamada (petición y respuesta; ±10 %) | 55 / 55 | ✓ |
| C: el servidor atendió lo que el cliente pidió (±concurrencia en vuelo y 1 % de desfase entre cuentas) | 85 / 90 | ⚠ |
| C: keep-alive real: ninguna conexión nueva en la ventana y a lo sumo `concurrencia` abiertas | 20 / 20 | ✓ |
| C: sin errores | 90 / 90 | ✓ |
| C: sin keep-alive: una conexión TCP por petición | 15 / 15 | ✓ |

Comprobaciones que fallaron (el punto se publica igualmente, marcado):

- **A: sin avisos de fiabilidad (proceso saturado, carga ajena)**
  - A etag N=1 C=12 T=5 → carga ajena: 0.90 núcleos gastados por procesos que no son del banco
  - A etag N=1 C=12 T=5 → carga ajena: 1.41 núcleos gastados por procesos que no son del banco
  - A etag N=1 C=12 T=5 → carga ajena: 0.85 núcleos gastados por procesos que no son del banco
- **B: sin avisos de fiabilidad (proceso saturado, carga ajena)**
  - B webhook N=1 H=120 → carga ajena: 2.80 núcleos gastados por procesos que no son del banco
  - B webhook N=1 H=120 → carga ajena: 2.59 núcleos gastados por procesos que no son del banco
  - B webhook N=1 H=120 → carga ajena: 0.59 núcleos gastados por procesos que no son del banco
- **C: el servidor atendió lo que el cliente pidió (±concurrencia en vuelo y 1 % de desfase entre cuentas)**
  - C flux c=256 → pedidas 24334, atendidas 21905
  - C flux c=256 → pedidas 14186, atendidas 15700
  - C flux c=256 → pedidas 19745, atendidas 16356

## La máquina y las versiones

|  |  |
|---|---:|
| CPU | Intel(R) Xeon(R) Processor @ 2.10GHz · 4 núcleos (1 hilo/núcleo) |
| Virtualización | KVM (full) |
| RAM | 15.7 GB |
| SO | Ubuntu 24.04.5 LTS · kernel 6.18.44-fc-v880 |
| Gobernador de frecuencia | no expuesto (máquina virtual) |
| Node | v22.23.5 (V8 12.4.254.21-node.57) |
| nats-server | nats-server: v2.14.5 |
| `@nats-io/*` | @nats-io/transport-node 3.4.0 · @nats-io/jetstream 3.4.0 · @nats-io/nkeys 2.0.3 |
| SDK de flux | @flux/sdk 0.1.0 (commit 9fa4982) |
| openssl (TLS) | OpenSSL 3.0.13 30 Jan 2024 (Library: OpenSSL 3.0.13 30 Jan 2024) |
| Carga de la máquina al empezar / al terminar la ejecución (1, 5, 15 min) | 2.17 1.62 1.12 / 7.57 12.25 13.22 |

## Lo que NO se mide

- **No hay red real.** Todo corre en `127.0.0.1`: la latencia de red (y sus pérdidas, y la pila TCP entre hosts) no está. Una llamada entre máquinas tarda más con HTTP y con NATS, y la diferencia entre ellos ahí es otra.
- **Un solo nodo de `nats-server`, sin cluster ni réplicas** (`replicas: 1`): una réplica 3 de JetStream añade ida y vuelta entre nodos a cada publicación con acuse.
- **Mismo host:** cliente, servidor y broker compiten por los mismos núcleos; la suma de CPU es comparable, el reparto entre procesos y la latencia bajo carga, menos.
- **Un solo CRM y un solo recurso** en el escenario A; un solo tipo de hecho en B; una sola operación en C. No se mide un esquema grande ni validación L3 ni firma (opcionales; el banco mide el camino por defecto).
- **TLS:** se mide aparte y solo en puntos escogidos (centro de cada escenario). Los bytes en la red no incluyen el sobrecoste de los registros TLS en ninguno de los dos lados; la CPU sí incluye el cifrado.
- **Resolución de CPU:** los procesos se leen por `schedstat` (ns) con el tic de 10 ms de `/proc/<pid>/stat` como contraste; un proceso casi ocioso mide casi cero y eso es lo que hay.
- **Los fallos** (receptor caído, 5xx, partición): no se miden. El reintento de flux (`backoff[0]` = 30 s) y el de un webhook (200 ms) son muy distintos por diseño.
- **No se mide el trabajo de la aplicación** (parsear, guardar en base de datos): los receptores solo anotan.

## Reproducir

```bash
node bench/run.mjs            # completo; tarda 83 min con la máquina tranquila
node bench/run.mjs --rapido   # prueba de humo, < 2 min
```
