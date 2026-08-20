// Taxonomía y clasificación de errores — 04-errors.md.

using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using Xunit;

namespace Flux.Tests;

public class ClassifierTests
{
    [Fact]
    public void UnErrorTipadoDeFluxSiempreGana()
    {
        // La aplicación sabe más que el SDK: solo ella conoce sus dependencias
        // — 04-errors.md §2.
        var retryable = Classifier.Default.Classify(
            new RetryableException("proveedor 503", code: "PROVEEDOR_CAIDO"));
        Assert.Equal(ErrorClass.Retryable, retryable.Class);
        Assert.Equal("PROVEEDOR_CAIDO", retryable.Code);

        var permanent = Classifier.Default.Classify(
            new PermanentException("pedido ya cancelado", code: "PEDIDO_YA_CANCELADO"));
        Assert.Equal(ErrorClass.Permanent, permanent.Class);
        Assert.Equal("PEDIDO_YA_CANCELADO", permanent.Code);

        var poison = Classifier.Default.Classify(new PoisonException("json roto"));
        Assert.Equal(ErrorClass.Poison, poison.Class);

        // Sin código explícito se cae al nombre del tipo: las métricas nunca quedan sin
        // etiqueta.
        Assert.Equal("PoisonException", poison.Code);
    }

    [Fact]
    public void PropagaElRetryAfterSoloEnLosRetryable()
    {
        var c = Classifier.Default.Classify(
            new RetryableException("429", retryAfter: TimeSpan.FromSeconds(5)));

        Assert.Equal(TimeSpan.FromSeconds(5), c.RetryAfter);

        // Un PERMANENT no tiene reintento del que hablar.
        Assert.Null(Classifier.Default.Classify(new PermanentException("no")).RetryAfter);
    }

    [Fact]
    public void AtraviesaErroresEnvueltos()
    {
        // Mejora sobre Node, cuyo `instanceof` solo mira el error de arriba: un
        // RetryableException envuelto por una capa intermedia sigue clasificándose bien.
        var envuelto = new InvalidOperationException("capa de repositorio",
            new RetryableException("deadlock", code: "DEADLOCK"));

        Assert.Equal("DEADLOCK", Classifier.Default.Classify(envuelto).Code);

        // Y también las ramas de un AggregateException, que es como llega un Task.WhenAll.
        var agregado = new AggregateException(
            new InvalidOperationException("otra cosa"),
            new PermanentException("regla de negocio", code: "REGLA"));

        Assert.Equal("REGLA", Classifier.Default.Classify(agregado).Code);
    }

    [Theory]
    [InlineData(429, ErrorClass.Retryable)]
    [InlineData(502, ErrorClass.Retryable)]
    [InlineData(503, ErrorClass.Retryable)]
    [InlineData(504, ErrorClass.Retryable)]
    [InlineData(400, ErrorClass.Permanent)]
    [InlineData(403, ErrorClass.Permanent)]
    [InlineData(404, ErrorClass.Permanent)]
    [InlineData(422, ErrorClass.Permanent)]
    public void ClasificaPorStatusHttp(int status, ErrorClass esperada)
    {
        // Reintentar un 400 es gastar 51 minutos para obtener exactamente la misma
        // respuesta — 04-errors.md §1.1.
        var c = Classifier.Default.Classify(new HttpStatusException(status));

        Assert.Equal(esperada, c.Class);
        Assert.Equal("HTTP_" + status, c.Code);
    }

    [Fact]
    public void LeeElStatusDeHttpRequestExceptionSinQueLaAplicacionHagaNada()
    {
        // Ventaja de .NET sobre Node y Go: el status viaja tipado en el BCL desde .NET 5,
        // así que no hace falta ni reflexión ni implementar una interfaz.
        var c = Classifier.Default.Classify(
            new HttpRequestException("upstream", null, HttpStatusCode.ServiceUnavailable));

        Assert.Equal(ErrorClass.Retryable, c.Class);
        Assert.Equal("HTTP_503", c.Code);
    }

    [Fact]
    public void UnFalloDeTransporteHttpSinRespuestaEsTransitorio()
    {
        // Sin StatusCode la petición nunca llegó a obtener respuesta: es "el mundo ahora
        // mismo", no el evento.
        var c = Classifier.Default.Classify(new HttpRequestException("no se pudo conectar"));

        Assert.Equal(ErrorClass.Retryable, c.Class);
    }

    [Fact]
    public void LeeElRetryAfterQueAnunciaLaDependencia()
    {
        var c = Classifier.Default.Classify(
            new HttpStatusException(503, "upstream", TimeSpan.FromSeconds(12)));

        Assert.Equal(TimeSpan.FromSeconds(12), c.RetryAfter);
    }

    [Theory]
    [InlineData(SocketError.ConnectionReset, "ECONNRESET")]
    [InlineData(SocketError.ConnectionRefused, "ECONNREFUSED")]
    [InlineData(SocketError.TimedOut, "ETIMEDOUT")]
    [InlineData(SocketError.HostUnreachable, "EHOSTUNREACH")]
    [InlineData(SocketError.NetworkUnreachable, "ENETUNREACH")]
    [InlineData(SocketError.TryAgain, "EAI_AGAIN")]
    public void LosErroresDeRedSonTransitoriosPorSemantica(SocketError error, string codigo)
    {
        // Se clasifica por SocketError, no por subcadenas del mensaje. En Windows los
        // códigos de libuv llevan prefijo WSA, y el port literal de la lista de Node
        // clasificaba el mismo corte de red como PERMANENT allí y RETRYABLE en Linux
        // — 04-errors.md §1.1.
        var c = Classifier.Default.Classify(new SocketException((int)error));

        Assert.Equal(ErrorClass.Retryable, c.Class);
        Assert.Equal(codigo, c.Code);

        // Y sin tope propio: un transitorio RECONOCIDO conserva sus 6 entregas.
        Assert.Equal(0, c.MaxAttempts);
        Assert.Equal(6, Classifier.EffectiveBudget(Protocol.DefaultMaxDeliver, c));
    }

    [Fact]
    public void UnNombreQueNoExisteNoEsTransitorio()
    {
        // El resolutor dice "no existe", no "reinténtalo": HostNotFound NO está en la lista.
        // Cae al default de lo desconocido, con su presupuesto acotado.
        var c = Classifier.Default.Classify(new SocketException((int)SocketError.HostNotFound));

        Assert.Equal("UNKNOWN", c.Code);
        Assert.Equal(Classifier.DefaultUnknownRetryBudget, c.MaxAttempts);
    }

    [Fact]
    public void LosTimeoutsSiguenLaPoliticaConfigurada()
    {
        Assert.Equal(ErrorClass.Retryable, Classifier.Default.Classify(new TimeoutException()).Class);
        Assert.Equal(ErrorClass.Retryable, Classifier.Default.Classify(new TaskCanceledException()).Class);

        var estricto = new Classifier(new ClassifierOptions { TimeoutPolicy = ErrorClass.Permanent });
        var c = estricto.Classify(new TimeoutException());

        Assert.Equal(ErrorClass.Permanent, c.Class);
        Assert.Equal("TIMEOUT", c.Code);
    }

    [Fact]
    public void ElDefaultDeLoDesconocidoEsRetryableAcotado()
    {
        // 04-errors.md §2.1: RETRYABLE con presupuesto de 2 entregas. Domina a las dos
        // alternativas obvias: PERMANENT manda a la DLQ un evento válido por un hipo de red;
        // RETRYABLE completo atasca la cola 51 min y amplifica el modo de fallo.
        var c = Classifier.Default.Classify(new InvalidOperationException("algo que nadie previó"));

        Assert.Equal(ErrorClass.Retryable, c.Class);
        Assert.Equal("UNKNOWN", c.Code);
        Assert.Equal(2, c.MaxAttempts);
        Assert.Equal(2, Classifier.EffectiveBudget(Protocol.DefaultMaxDeliver, c));
    }

    [Fact]
    public void ElPresupuestoAcotadoEsConfigurable()
    {
        var c = new Classifier(new ClassifierOptions { UnknownRetryBudget = 3 })
            .Classify(new InvalidOperationException("boom"));

        Assert.Equal(3, c.MaxAttempts);
    }

    [Theory]
    [InlineData(UnknownPolicy.Permanent, ErrorClass.Permanent, 0)]
    [InlineData(UnknownPolicy.Retryable, ErrorClass.Retryable, 0)]
    [InlineData(UnknownPolicy.RetryableBounded, ErrorClass.Retryable, 2)]
    public void LasTresPoliticasDeLoDesconocido(UnknownPolicy politica, ErrorClass clase, int tope)
    {
        var c = new Classifier(new ClassifierOptions { UnknownErrorPolicy = politica })
            .Classify(new InvalidOperationException("boom"));

        Assert.Equal(clase, c.Class);
        Assert.Equal(tope, c.MaxAttempts);

        // Solo la política acotada recorta; las otras dos dejan mandar al max_deliver.
        Assert.Equal(
            tope == 0 ? Protocol.DefaultMaxDeliver : tope,
            Classifier.EffectiveBudget(Protocol.DefaultMaxDeliver, c));
    }

    [Fact]
    public void ElPresupuestoNoRecortaLosRetryableReconocidos()
    {
        // La razón de que el tope viaje en la clasificación y no en max_deliver: éste es
        // por consumidor, y bajarlo a 2 recortaría también los transitorios reconocidos
        // — 04-errors.md §2.1.
        var conocido = Classifier.Default.Classify(new SocketException((int)SocketError.ConnectionReset));
        var desconocido = Classifier.Default.Classify(new InvalidOperationException("?"));

        Assert.Equal(6, Classifier.EffectiveBudget(Protocol.DefaultMaxDeliver, conocido));
        Assert.Equal(2, Classifier.EffectiveBudget(Protocol.DefaultMaxDeliver, desconocido));
    }

    [Fact]
    public void LasReglasDeLaAplicacionSeEvaluanAntesQueLoDemas()
    {
        var clasificador = new Classifier(new ClassifierOptions
        {
            Rules = new Func<Exception, Classification?>[]
            {
                e => e.Message.Contains("cuota", StringComparison.Ordinal)
                    ? new Classification(ErrorClass.Retryable, "CUOTA") { RetryAfter = TimeSpan.FromMinutes(10) }
                    : null,
            },
        });

        var c = clasificador.Classify(new HttpStatusException(400, "cuota agotada"));

        // La regla gana sobre el 400, que por sí solo sería PERMANENT.
        Assert.Equal("CUOTA", c.Code);
        Assert.Equal(TimeSpan.FromMinutes(10), c.RetryAfter);

        // Pero no gana sobre un error tipado de flux.
        Assert.Equal(
            "EXPLICITO",
            clasificador.Classify(new PermanentException("cuota agotada", code: "EXPLICITO")).Code);
    }

    [Fact]
    public void UnaListaDeReglasMutadaDespuesNoCambiaLaPolitica()
    {
        var reglas = new List<Func<Exception, Classification?>>();
        var clasificador = new Classifier(new ClassifierOptions { Rules = reglas });

        reglas.Add(_ => new Classification(ErrorClass.Poison, "INYECTADA"));

        Assert.Equal("UNKNOWN", clasificador.Classify(new InvalidOperationException("boom")).Code);
    }

    [Fact]
    public void ClassifyDeNullNoInventaUnaClase()
    {
        var c = Classifier.Default.Classify(null);

        Assert.Equal("NIL_ERROR", c.Code);
    }
}
