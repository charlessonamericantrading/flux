#!/usr/bin/env node
/**
 * Los permisos de publicación y de suscripción que `gen-acl.mjs` da a unos servicios del ejemplo: lo que usa la prueba de «cada permiso es
 * NECESARIO» para comprobar que su matriz cubre TODOS (si la lista crece, la matriz falla en lugar de dejar un permiso sin medir).
 *
 *   node permisos.mjs crm-api tienda-web  →  {"crm-api":{"publish":[…],"subscribe":[…]},…}
 */
import { permisosJson } from "../../../scripts/lib/nats-harness.mjs";

const nombres = process.argv.slice(2);
const todos = permisosJson();
process.stdout.write(
  `${JSON.stringify(Object.fromEntries(todos.filter((s) => nombres.includes(s.name)).map((s) => [s.name, { publish: s.publish, subscribe: s.subscribe }])))}\n`,
);
