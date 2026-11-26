# Evidencia del plan v1.1

Experimentos **ad-hoc** contra un `nats-server` real. Respaldan los hallazgos y las
decisiones de diseño de [`../PLAN.md`](../PLAN.md) (Apéndice A).

**Esta carpeta es temporal.** Cada fase convierte lo que usa en casos de
`conformance/cases/` y lo borra de aquí. Al cerrar la Fase 3 no debe quedar nada.

```bash
(cd conformance && npm install)
docs/plan-v1.1/evidencia/ejecutar.sh
```

Necesita `nats-server` 2.14.x en el `PATH`, o su ruta en `NATS_SERVER`. Sin Docker:

```bash
GOBIN=$PWD/bin go install github.com/nats-io/nats-server/v2@v2.14.5
NATS_SERVER=$PWD/bin/nats-server docs/plan-v1.1/evidencia/ejecutar.sh
```

| Fichero | Qué demuestra | Lo sustituye |
|---|---|---|
| `acl-generada.mjs` | Con la ACL que genera hoy `scripts/gen-acl.mjs`, un servicio no puede consumir lo concedido y sí puede leer lo no concedido | Fase 1 |
| `candidata.conf` | La forma de permisos que debe generar `gen-acl.mjs` v2 | Fase 1 |
| `diseno-candidato.mjs` | ACL candidata, estado replicado y petición/respuesta | Fases 1, 2 y 3 |

Una línea `FALLA [debe fallar] …` es el resultado **correcto**: es una denegación que
se esperaba.
