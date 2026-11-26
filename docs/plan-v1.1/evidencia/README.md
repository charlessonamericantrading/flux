# Evidencia del plan v1.1

Experimentos **ad-hoc** contra un `nats-server` real. Respaldan los hallazgos y las
decisiones de diseño de [`../PLAN.md`](../PLAN.md) (Apéndice A).

**Esta carpeta es temporal.** Cada fase convierte lo que usa en casos de
`conformance/cases/` y lo borra de aquí. Al cerrar la Fase 3 no debe quedar nada.

**La Fase 1 ya lo hizo.** La evidencia de seguridad (la ACL v1 rota, H1 y H2, y la ACL
candidata con inbox privado y permisos exactos) es ahora
[`conformance/cases/security-acl.json`](../../../conformance/cases/security-acl.json), que
ejecuta [`conformance/security.mjs`](../../../conformance/security.mjs) contra la ACL que
genera `scripts/gen-acl.mjs`: `acl-generada.mjs` y la parte de ACL de `diseno-candidato.mjs`
se han borrado. La ACL v1 sigue congelada en `conformance/fixtures/acl-v1.conf` (caso S9).

**La Fase 2 también.** El estado replicado (A-9 … A-14 y lo que midió la fase: A-31 … A-50) es ahora
[`conformance/cases/state-broker.json`](../../../conformance/cases/state-broker.json) y
[`state-semantics.json`](../../../conformance/cases/state-semantics.json), que ejecuta
[`conformance/state-broker.mjs`](../../../conformance/state-broker.mjs) contra un `nats-server` real
(uno suelto, uno con ACL por usuario y un cluster de tres nodos): la parte de estado de
`diseno-candidato.mjs` y el observador de estado de `candidata.conf` se han borrado.

Lo que queda es lo de la **Fase 3**:

```bash
(cd conformance && npm install)
docs/plan-v1.1/evidencia/ejecutar.sh
```

Necesita `nats-server` 2.14.x en el `PATH`, o su ruta en `NATS_SERVER`. Sin Docker:

```bash
GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5
NATS_SERVER=$PWD/bin/nats-server docs/plan-v1.1/evidencia/ejecutar.sh
```

Escucha en el puerto 4234 (el de `candidata.conf` y `diseno-candidato.mjs`; `NATS_URL` en
el script de Node, `port:` en la config, si hay que cambiarlo).

| Fichero | Qué demuestra | Lo sustituye |
|---|---|---|
| `candidata.conf` | Los usuarios del experimento: quien llama y el dueño que atiende peticiones, con inbox privado. Es la forma que `gen-acl.mjs` debe llegar a generar con la clave `calls` | Fase 3 |
| `diseno-candidato.mjs` | Petición/respuesta (A-15 … A-18) | Fase 3 |
| `ejecutar.sh` | Arranca el servidor con `candidata.conf` y lanza el experimento | Fase 3 |

Una línea `FALLA [debe fallar] …` es el resultado **correcto**: es una denegación que
se esperaba.
