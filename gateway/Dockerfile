# El gateway HTTP de borde de flux (specification/12-edge.md). Depende del SDK de Node del repo (`@flux/sdk`: file:../sdk-node), así que
# se construye con la RAÍZ del repositorio como contexto:
#
#   docker build -f gateway/Dockerfile -t flux-gateway .
#   docker run --rm -p 8080:8080 -v "$PWD/gateway/examples/gateway.json:/etc/flux/gateway.json:ro" \
#     -e TIENDA_WEB_NKEY_SEED -e CATALOGO_WEB_NKEY_SEED -e PASARELA_WEB_NKEY_SEED \
#     -e TIENDA_WEB_ENTREGA_SECRETO -e PASARELA_HOOK_SECRETO -e GITHUB_HOOK_SECRETO flux-gateway
#
# La imagen NO lleva ningún secreto ni configuración: `gateway.json` se monta (no contiene secretos: solo nombres de variables de entorno
# y SHA-256 de tokens) y los secretos llegan por el entorno. Sin TLS: escucha en HTTP dentro de una red privada, detrás de un proxy que lo
# termine, o se le dan `listen.tls.certFile` y `keyFile` (rutas montadas) — specification/12-edge.md §7.2.

# ── 1. El SDK: se compila y se queda solo con sus dependencias de ejecución ───────────────────────────────────────────────────
FROM node:22-alpine AS sdk
WORKDIR /app/sdk-node
COPY sdk-node/package.json sdk-node/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY sdk-node/tsconfig.json ./
COPY sdk-node/src ./src
RUN npm run build && npm prune --omit=dev --no-audit --no-fund

# ── 2. El gateway ─────────────────────────────────────────────────────────────────────────────────────────────────────────────
FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY --from=sdk /app/sdk-node /app/sdk-node
WORKDIR /app/gateway
COPY gateway/package.json gateway/package-lock.json ./
# `@flux/sdk` es un enlace a ../sdk-node (ya compilado arriba): no hay que publicarlo en ningún registro.
RUN npm ci --omit=dev --no-audit --no-fund
COPY gateway/src ./src

USER node
EXPOSE 8080
# /healthz no exige autenticación ni devuelve datos: dice si los clientes de borde siguen conectados al broker.
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["node", "src/index.mjs"]
CMD ["--config", "/etc/flux/gateway.json"]
