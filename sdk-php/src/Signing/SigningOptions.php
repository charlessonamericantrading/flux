<?php

declare(strict_types=1);

namespace Flux\Signing;

/**
 * Configuración de la firma Ed25519 — **extensión OPCIONAL de v1**.
 * Contrato normativo: specification/07-signing.md
 *
 * El default es no firmar y no verificar: un evento sin firma sigue siendo válido y un SDK
 * conforme no necesita implementar la extensión (07-signing.md, cabecera).
 */
final readonly class SigningOptions
{
    /**
     * @param string|null $privateKey Clave privada Ed25519 para firmar al publicar: PEM
     *        PKCS#8, o base64 cruda. `null` para solo verificar.
     *        **NUNCA se versiona** — 06-security.md §2.
     * @param string|null $keyId Id de la clave, formato `<servicio>-<n>`. Obligatorio si se
     *        firma: sin él un verificador no sabe qué clave pública usar.
     * @param array<string,string> $publicKeys `signkeyid` → PEM SPKI o base64 cruda.
     *
     *        ⚠️ **Incluye aquí las claves RETIRADAS mientras exista algún evento firmado
     *        con ellas** (mínimo 90 días, la retención de la DLQ). Retirar una clave impide
     *        **emitir** con ella, no **verificar** lo ya emitido; tratar una retirada como
     *        inválida convierte una rotación rutinaria en la invalidación retroactiva de
     *        todo el historial — 07-signing.md §6.
     * @param VerificationMode $verify Política de verificación al consumir.
     */
    public function __construct(
        public ?string $privateKey = null,
        public ?string $keyId = null,
        public array $publicKeys = [],
        public VerificationMode $verify = VerificationMode::Off,
    ) {
    }
}
