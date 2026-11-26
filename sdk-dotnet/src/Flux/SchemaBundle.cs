// Bundle de JSON Schemas desplegado CON el servicio.
// Contrato normativo: specification/00-protocol.md §5, "Resolución de esquemas: bundle, no HTTP".
//
// El `dataschema` de un evento es una URI, pero un SDK L3 NO DEBE resolverla por red al
// publicar. La razón no es el coste de la petición —que también—, sino la ventana de
// inconsistencia: una caché con TTL hace que dos servicios validen contra versiones
// distintas del MISMO esquema durante los minutos que dura el TTL, y ese fallo no produce
// ningún error: produce dos verdades.
//
// Este tipo vive en el paquete BASE y no en Flux.Validation a propósito: `FluxBus` lo
// necesita para resolver el `dataschema` exacto de cada subject, y eso no requiere validar
// nada ni, por tanto, ninguna librería de JSON Schema.

using System.Text.Json;

namespace Flux;

/// <summary>
/// El <c>schemas/bundle.json</c> que genera <c>scripts/bundle-schemas.mjs</c>, ya leído.
/// </summary>
/// <remarks>
/// Se pasa como DATO en <see cref="ValidationOptions.Bundle"/>; el SDK no lo descarga ni lo
/// busca por su cuenta. Lo normal es empaquetarlo como recurso incrustado o copiarlo junto
/// al ejecutable:
/// <code>
/// var bundle = SchemaBundle.FromFile("schemas/bundle.json");
/// </code>
/// Es inmutable y seguro de compartir entre hilos.
/// </remarks>
public sealed class SchemaBundle
{
    private SchemaBundle(
        IReadOnlyDictionary<string, string> subjects,
        IReadOnlyDictionary<string, string> schemas,
        IReadOnlyDictionary<string, string> state,
        IReadOnlyDictionary<string, (string Request, string Reply)> rpc)
    {
        Subjects = subjects;
        Schemas = schemas;
        State = state;
        Rpc = rpc;
    }

    /// <summary>contrato de estado (<c>clientes.cliente.v1</c>) → URI del esquema de versión más alta de su mayor (10-state.md §10).</summary>
    public IReadOnlyDictionary<string, string> State { get; }

    /// <summary>operación (<c>clientes.cliente.v1.obtener</c>) → URI del esquema de la petición y de la respuesta, de la versión más alta de su mayor (11-request-reply.md §11.1).</summary>
    public IReadOnlyDictionary<string, (string Request, string Reply)> Rpc { get; }

    /// <summary>subject → URI del esquema con el MINOR más alto de su mayor.</summary>
    public IReadOnlyDictionary<string, string> Subjects { get; }

    /// <summary>
    /// URI → el JSON Schema, tal cual, sin interpretar.
    /// </summary>
    /// <remarks>
    /// Se guarda el TEXTO y no un árbol ya parseado porque quien lo consume es un
    /// compilador de esquemas que vive en otro paquete (<c>Flux.Validation</c>), y darle el
    /// texto evita que el paquete base tenga que conocer su modelo de objetos.
    /// </remarks>
    public IReadOnlyDictionary<string, string> Schemas { get; }

    /// <summary>Cuántos esquemas trae.</summary>
    public int Count => Schemas.Count;

    /// <summary>Lee el bundle desde su texto JSON.</summary>
    /// <param name="json">Contenido de <c>bundle.json</c>.</param>
    /// <returns>El bundle.</returns>
    /// <exception cref="ArgumentException">No es JSON válido, o no trae ningún esquema.</exception>
    public static SchemaBundle FromJson(string json)
    {
        ArgumentException.ThrowIfNullOrEmpty(json);

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException e)
        {
            throw new ArgumentException(
                "el bundle de esquemas no es JSON válido. Regenéralo con " +
                "`node scripts/bundle-schemas.mjs`", nameof(json), e);
        }

        using (document)
        {
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                throw new ArgumentException(
                    "el bundle de esquemas no es un objeto JSON", nameof(json));
            }

            var subjects = new Dictionary<string, string>(StringComparer.Ordinal);
            if (root.TryGetProperty("subjects", out var subjectsNode) &&
                subjectsNode.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in subjectsNode.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.String)
                    {
                        subjects[property.Name] = property.Value.GetString()!;
                    }
                }
            }

            var schemas = new Dictionary<string, string>(StringComparer.Ordinal);
            if (root.TryGetProperty("schemas", out var schemasNode) &&
                schemasNode.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in schemasNode.EnumerateObject())
                {
                    schemas[property.Name] = property.Value.GetRawText();
                }
            }

            if (schemas.Count == 0)
            {
                // Un bundle vacío no es un bundle: con Mode = Strict haría que TODO evento
                // fallara con SchemaNotFoundException, y el operador buscaría el problema
                // en el evento en vez de en el fichero que no se generó.
                throw new ArgumentException(
                    "el bundle no contiene ningún esquema (clave `schemas` ausente o vacía). " +
                    "Regenéralo con `node scripts/bundle-schemas.mjs`", nameof(json));
            }

            var state = new Dictionary<string, string>(StringComparer.Ordinal);
            if (root.TryGetProperty("state", out var stateNode) && stateNode.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in stateNode.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.String)
                    {
                        state[property.Name] = property.Value.GetString()!;
                    }
                }
            }

            var rpc = new Dictionary<string, (string Request, string Reply)>(StringComparer.Ordinal);
            if (root.TryGetProperty("rpc", out var rpcNode) && rpcNode.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in rpcNode.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.Object &&
                        property.Value.TryGetProperty("request", out var request) && request.ValueKind == JsonValueKind.String &&
                        property.Value.TryGetProperty("reply", out var reply) && reply.ValueKind == JsonValueKind.String)
                    {
                        rpc[property.Name] = (request.GetString()!, reply.GetString()!);
                    }
                }
            }

            return new SchemaBundle(subjects, schemas, state, rpc);
        }
    }

    /// <summary>Lee el bundle desde un fichero.</summary>
    /// <param name="path">Ruta a <c>bundle.json</c>.</param>
    /// <returns>El bundle.</returns>
    public static SchemaBundle FromFile(string path) => FromJson(File.ReadAllText(path));

    /// <summary>
    /// La URI de <c>dataschema</c> de un subject, o <see langword="null"/> si el bundle no
    /// lo conoce.
    /// </summary>
    /// <remarks>
    /// El bundle resuelve el MINOR exacto y no el <c>.0.0</c> del mayor: dentro de un mayor
    /// todo es BACKWARD-compatible, así que el MINOR más alto acepta todo lo que aceptan los
    /// anteriores — 00-protocol.md §5.
    /// </remarks>
    /// <param name="subject">Subject de NATS.</param>
    /// <returns>La URI, o <see langword="null"/>.</returns>
    public string? SchemaUriFor(string subject) =>
        Subjects.TryGetValue(subject, out var uri) ? uri : null;

    /// <summary>La URI de <c>dataschema</c> de un contrato de estado, o <see langword="null"/> si el bundle no lo conoce. Sin red.</summary>
    public string? StateSchemaUriFor(string contract) =>
        State.TryGetValue(contract, out var uri) ? uri : null;

    /// <summary>La URI de <c>dataschema</c> de la petición o de la respuesta de una operación, o <see langword="null"/>. Sin red.</summary>
    public string? RpcSchemaUriFor(string operation, bool reply) =>
        Rpc.TryGetValue(operation, out var uris) ? (reply ? uris.Reply : uris.Request) : null;
}
