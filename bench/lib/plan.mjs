/**
 * El PLAN de medidas: qué puntos se miden, con qué ventana y cuántas repeticiones.
 *
 * Un GRUPO comparte un `nats-server` aprovisionado (los durables de un tamaño N de consumidores), y dentro de él las
 * repeticiones se INTERCALAN entre los puntos (rep 1 de todos, rep 2 de todos…): si la máquina deriva durante la hora larga de
 * medida (térmica, vecinos, caché), la deriva se reparte entre los métodos en vez de cargarla sobre el último.
 *
 * Diseño de los barridos del escenario A (centro: N=100 consumidores, C=12 cambios/min, T=5 s): se varía UN factor cada vez.
 */

const A = (metodo, n, c, t, extra = {}) => ({ escenario: "A", metodo, n, c, t, ...extra });
const B = (metodo, n, h, extra = {}) => ({ escenario: "B", metodo, n, h, ...extra });
const C = (metodo, conc, extra = {}) => ({ escenario: "C", metodo, conc, ...extra });

export function plan({ rapido }) {
  if (rapido) {
    // Prueba de humo: un punto por camino de código, ventanas cortas, una repetición. NO son resultados.
    const wA = { ventanaS: 7, calentamientoS: 1 };
    return {
      reps: 1,
      centroA: { n: 10, c: 60, t: 1 },
      centroB: { n: 10, h: 120 },
      grupos: [
        { id: "A-N10", escenario: "A", n: 10, puntos: ["ingenuo", "etag", "flux"].map((m) => A(m, 10, 60, 1, wA)) },
        { id: "B-N10", escenario: "B", n: 10, puntos: ["webhook", "flux"].map((m) => B(m, 10, 120, { ventanaS: 5, calentamientoS: 1 })) },
        {
          id: "C", escenario: "C",
          puntos: ["http-ka", "http-cerrar", "nats-crudo", "flux", "flux-otel"].map((m) => C(m, 16, { ventanaS: 2, calentamientoS: 1 })),
        },
        // Un solo punto con TLS: ejercita el certificado, el `Agent` https y el listener TLS de nats-server.
        { id: "C-TLS", escenario: "C", tls: true, puntos: ["http-ka", "flux"].map((m) => C(m, 4, { ventanaS: 2, calentamientoS: 1, tls: true })) },
      ],
    };
  }
  const grupos = [];
  // Ventanas: la de A con sondeo cada T debe contener el tiempo de gracia (1,05·T + 0,5 s) y aún ≥ 9 s de cambios contados.
  const vA = (t) => (t >= 30 ? { ventanaS: 70, calentamientoS: 3 } : { ventanaS: 15 + Math.ceil(t * 1.05 + 0.5), calentamientoS: 3 });
  for (const n of [1, 10, 100, 1000]) {
    const puntos = [A("etag", n, 12, 5, vA(5)), A("flux", n, 12, 5, vA(5))];
    if (n >= 100) puntos.unshift(A("ingenuo", n, 12, 5, vA(5)));
    if (n === 100) {
      for (const c of [1, 60, 600]) puntos.push(A("etag", n, c, 5, vA(5)), A("flux", n, c, 5, vA(5)));
      for (const t of [1, 30]) puntos.push(A("etag", n, 12, t, vA(t)));
    }
    grupos.push({ id: `A-N${n}`, escenario: "A", n, puntos });
  }
  grupos.push({ id: "A-TLS", escenario: "A", n: 100, tls: true, puntos: [A("etag", 100, 12, 5, { ...vA(5), tls: true }), A("flux", 100, 12, 5, { ...vA(5), tls: true })] });

  const wB = { ventanaS: 12, calentamientoS: 2 };
  for (const n of [1, 10, 100, 1000]) {
    const puntos = ["webhook", "flux"].map((m) => B(m, n, 120, wB));
    if (n === 100) puntos.push(...["webhook", "flux"].map((m) => B(m, n, 600, wB)));
    grupos.push({ id: `B-N${n}`, escenario: "B", n, puntos });
  }
  grupos.push({ id: "B-TLS", escenario: "B", n: 100, tls: true, puntos: ["webhook", "flux"].map((m) => B(m, 100, 120, { ...wB, tls: true })) });

  const wC = { ventanaS: 6, calentamientoS: 1.5 };
  grupos.push({
    id: "C", escenario: "C",
    puntos: [1, 16, 256].flatMap((conc) => ["http-ka", "http-cerrar", "nats-crudo", "flux", "flux-otel"].map((m) => C(m, conc, wC))),
  });
  grupos.push({ id: "C-TLS", escenario: "C", tls: true, puntos: ["http-ka", "nats-crudo", "flux"].map((m) => C(m, 16, { ...wC, tls: true })) });
  return { reps: 5, centroA: { n: 100, c: 12, t: 5 }, centroB: { n: 100, h: 120 }, grupos };
}
