# `bench/` — cuánto cuesta enterarse, avisar y llamar: HTTP frente a flux

Un banco de medida que **compara con números** lo que hoy se hace entre servicios de un mismo ecosistema (un CRM, varias webs y
servicios) con APIs HTTP, polling y webhooks, frente a su equivalente en flux sobre un `nats-server` real. Es la Fase 5.1 del plan
([`docs/plan-v1.1/PLAN.md`](../docs/plan-v1.1/PLAN.md)). **Se publica lo que salga, favorezca o no a flux.**

Los resultados están en [`RESULTS.md`](RESULTS.md), que **genera el runner**: no se edita a mano, y el `README.md` raíz solo cita cifras de ese
fichero.

## Reproducirlo con un comando

```bash
node bench/run.mjs            # la malla completa (cada punto, 5 repeticiones). Escribe bench/RESULTS.md y bench/datos/completo.json
node bench/run.mjs --rapido   # prueba de humo, < 2 min. Escribe bench/resultados/RESULTS.rapido.md; NO toca RESULTS.md
```

Requisitos: Node ≥ 20, `nats-server` (`NATS_SERVER=/ruta/a/nats-server`, o en el `PATH`), `openssl` (los puntos con TLS) y las dependencias de
`conformance/`, `cli/` y `sdk-node/` instaladas. Si faltan, el runner lo dice y `node bench/run.mjs --preparar` las instala y compila el SDK.
Puertos: `FLUX_TEST_PORTS=desde-hasta` (por defecto `4700-4799`).

Otras opciones: `--escenario a,b,c`, `--reps N`, `--reintentos N`, `--salida FICHERO`, `--datos FICHERO`, y `--solo-informe` (regenera `RESULTS.md`
desde `datos/completo.json` sin medir). `node bench/autoprueba.mjs` estropea el banco a propósito (ver «Cómo se sabe que el banco mide bien»).
`node bench/comprobar-resultados.mjs [fichero]` comprueba la **estructura** de un `RESULTS.md` (lo que ejecuta el CI).

### Cuánto tarda y qué comprobar antes de lanzar la completa

La corrida completa son 50 puntos × 5 repeticiones = **250 repeticiones** (`RESULTS.md` dice cuánto tardó la última: ver «Ejecución»). El modo
rápido tarda **~75 s**.

Antes de lanzar la completa **con la máquina en calma**:

1. `uptime`: la carga media de 1 min debe estar por debajo de ~0.3. El banco mide cada repetición contra la carga de OTROS (`/proc/stat` menos
   sus procesos) y repite hasta 2 veces las que pasen de 0.25 núcleos ajenos; con la máquina ocupada eso alarga la corrida sin arreglarla.
2. Que no haya compilaciones, pruebas de otros agentes ni otro `nats-server` por ahí (`ps aux --sort=-%cpu | head`).
3. `node bench/run.mjs --rapido` limpio, y `node bench/autoprueba.mjs` (~1 min) en verde.
4. Sin pausar la máquina (suspensión, `cpulimit`…): la corrida dura más de una hora.
5. Si se ejecuta en otra máquina, el modelo de CPU, los núcleos y las versiones quedan escritos en `RESULTS.md`: no mezcles dos máquinas en un
   mismo fichero.

`RESULTS.md` dice, además, con qué carga se midió (`Calidad de la medida`) y, si hubo carga ajena apreciable, lo avisa arriba del todo y separa
las cifras **robustas** a la carga de las **sensibles** (†).

## Los tres escenarios

| | Línea base HTTP | flux |
|---|---|---|
| **A. Enterarse de un cambio** — N consumidores, C cambios por minuto | cada consumidor sondea `GET /clientes/cli-1` cada T s con keep-alive: **ingenuo** (siempre 200) y **con `If-None-Match`** (304): el sondeo inteligente es la línea base justa | cada consumidor es un servicio con su `bus.state.watch` |
| **B. Avisar a muchos** — 1 productor, N receptores, H hechos por minuto | N webhooks por hecho: un `POST` por receptor, keep-alive, hasta 3 intentos, sin cola | 1 evento publicado y N consumidores durables (un servicio cada uno) |
| **C. Llamada con respuesta** — concurrencia 1, 16 y 256 | `node:http` con keep-alive (la línea base justa) y sin él (lo que hace un cliente sin configurar) | `bus.request` / `bus.respond`; y dos líneas más para repartir el coste: **NATS crudo** (core NATS sin el SDK) y **flux con OpenTelemetry presente** (experimento) |

Cada uno mide: **peticiones o mensajes emitidos**, **segundos de CPU de cada proceso** (clientes, servidor y `nats-server`), **bytes en la red**, **latencia
p50/p99/máx** y, en A, el **tiempo hasta enterarse** (lo que el sondeo pierde: con periodo T, de media T/2). Cada punto se repite 5 veces y se
publica la mediana y el rango.

### El entorno de flux es el seguro, no uno abierto

El `nats-server` se arranca con la misma forma que `scripts/dev-secure.mjs`: **una nkey por servicio**, las **ACL que genera `scripts/gen-acl.mjs`** a partir de un manifiesto
(inbox privado `_INBOX.<servicio>.>`, permisos exactos de cada durable, `allow_responses` acotado), los streams y durables que crea **`flux provision`** con credenciales de
operación (los servicios no pueden crear nada) y, en los puntos marcados, TLS. Los servicios usan `connect()` del SDK de Node compilado (`sdk-node/dist`), sin validación L3 ni
firma (opcionales): se mide el camino por defecto.

Cada consumidor es **un servicio distinto** (su durable, su conexión): N = 1000 son 1000 conexiones NATS, y en la línea base 1000 conexiones HTTP, una por consumidor.

### Qué hace justa la línea base HTTP

- **Keep-alive de verdad.** Un `Agent` por consumidor con `keepAlive`, y el servidor con `keepAliveTimeout` de 120 s: el de Node es de 5 s, y con un sondeo cada 30 s cada
  sondeo pagaría un handshake. Se **cuentan las conexiones TCP** y se exige que en la ventana no haya ninguna nueva.
- **Sondeo inteligente** (`If-None-Match` → `304`) además del ingenuo. Se exige que reciba `304`.
- **Webhooks con reintentos mínimos** (3 intentos, 200/400 ms), keep-alive hacia cada receptor y envío en paralelo.
- **El mismo trabajo en el receptor**: parsear el cuerpo y anotar qué llegó. Nada más (no hay base de datos).
- La latencia del cambio en A se mide **desde que el CRM empieza a escribir** (en flux, desde antes del `put`, que persiste en JetStream; en HTTP, un cambio en memoria): es una
  ventaja del lado HTTP que se deja a propósito.

### Cómo se mide

- **CPU:** `/proc/<pid>/task/<tid>/schedstat` (ns en CPU de cada hilo) con `/proc/<pid>/stat` (tics de 10 ms) como contraste, para **cada proceso**: cada rol es un proceso aparte
  (CRM o productor, procesos de consumidores —hasta 250 por proceso, y la CPU se suma—, `nats-server`).
- **Bytes en la red:** de aplicación y sin cabeceras TCP/IP. HTTP: `bytesRead + bytesWritten` de cada socket del lado servidor; NATS: `in_bytes + out_bytes` de `/varz`.
- **Mensajes:** HTTP: peticiones recibidas. NATS: `in_msgs` de `/varz` (publicaciones, peticiones de mensajes, acuses…).
- **Latencia:** relojes `CLOCK_MONOTONIC` (`process.hrtime`), común a todos los procesos; el cruce de «cuándo cambió» con «cuándo lo vio cada consumidor» lo hace el runner.
- **Calentamiento:** las conexiones se abren y se comprueban **antes** de medir; después, 1–3 s de calentamiento descartado; la ventana de CPU y bytes empieza tras él.
  Los cambios cuentan para el tiempo hasta enterarse solo si ocurrieron al menos T·1.05 + 0.5 s antes del final (si no, el sondeo aún no tuvo ocasión de verlos).
- Los **cambios** se programan con un PRNG con semilla (intervalo ±50 %, primer cambio aleatorio) y los sondeadores tienen **fase aleatoria**: una flota real no está sincronizada.
- Las repeticiones de un grupo se **intercalan** entre los métodos (rep 1 de todos, rep 2 de todos…): si la máquina deriva, la deriva se reparte.

### Cómo se sabe que el banco mide bien

Cada repetición lleva sus **comprobaciones** (`lib/comprobar.mjs`) y `RESULTS.md` cuenta cuántas pasan:

- el **keep-alive es de verdad** (cero conexiones TCP nuevas en la ventana; una viva por consumidor; en C sin keep-alive, una por petición);
- el sondeo con ETag **recibe `304`** y el ingenuo nunca;
- lo que emitieron los clientes es lo que **recibió** el servidor; sin errores ni sondeos omitidos;
- **cada mensaje se entrega una vez** a cada receptor: ni perdidos, ni duplicados, ni ajenos a la corrida;
- **ningún proceso del banco está saturado** (> 85 % de un núcleo de Node) ni hay carga ajena apreciable (> 0.25 núcleos, mirando `/proc/stat` y quiénes eran);
  una repetición así se repite (por defecto hasta 2 veces) y, si sigue mal, se publica marcada.

Y `node bench/autoprueba.mjs` **estropea el banco a propósito** —sondeadores sin keep-alive, un servidor que ignora `If-None-Match`, un cliente saturado, un receptor que falla una de
cada tres, un consumidor que se traga los eventos, un «keep-alive» que no lo es— y exige que la comprobación correspondiente **salte**, y que los controles sin sabotaje pasen.
Una comprobación que solo se ha visto en verde no demuestra nada.

## Estructura

```
bench/
  run.mjs                  el runner (--rapido, --solo-informe, --preparar…)
  autoprueba.mjs           estropea el banco a propósito y exige que lo detecte
  comprobar-resultados.mjs comprueba la ESTRUCTURA de un RESULTS.md (no sus valores); lo usa el CI
  worker.mjs               un proceso del banco, gobernado por IPC
  roles/                   un fichero por proceso: a-crm-http, a-pollers, a-crm-flux, a-watchers, b-hook-prod, b-hook-recep,
                           b-evento-prod, b-evento-cons, c-http-servidor, c-http-cliente, c-flux-*, c-nats-*
  escenarios/              a-enterarse, b-avisar, c-llamada: lanzan los roles, miden la ventana y calculan las métricas
  lib/                     entorno (nats-server seguro, máquina, versiones), medida (ventana, CPU, ajenos), comprobar, plan, informe, estadística
  datos/completo.json      los datos de la última corrida completa (RESULTS.md se regenera de aquí con --solo-informe)
  lectura.md               la lectura escrita a mano sobre esa corrida (el resto de RESULTS.md es generado)
  RESULTS.md               generado
```

## Lo que NO se mide

Está al final de `RESULTS.md`, y conviene leerlo antes de citar una cifra: **no hay red real** (todo en `127.0.0.1`), **un solo nodo** de `nats-server` sin cluster ni réplicas, **mismo host** para
clientes, servidor y broker, un solo recurso/operación por escenario, y ni los fallos (un receptor caído) ni el trabajo de la aplicación se miden.

## Añadir un escenario o un método

1. Un rol por proceso en `roles/` (`crear(cfg)` → `{ arranca, snap, fin, cerrar }`; `snap()` devuelve contadores numéricos acumulados y `fin()` los registros).
2. Un módulo en `escenarios/` que arme los procesos, llame a `correr()` y devuelva las métricas de la repetición; sus comprobaciones, en `lib/comprobar.mjs`.
3. Sus puntos en `lib/plan.mjs` y su tabla en `lib/informe.mjs`. Y un sabotaje en `autoprueba.mjs` por cada cosa que la comprobación nueva dice detectar.
