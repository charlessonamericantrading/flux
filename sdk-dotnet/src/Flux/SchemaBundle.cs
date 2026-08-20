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
        IReadOnlyDictionary<string, string> schemas)
    {
        Subjects = subjects;
        Schemas = schemas;
    }

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

            return new SchemaBundle(subjects, schemas);
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
}
