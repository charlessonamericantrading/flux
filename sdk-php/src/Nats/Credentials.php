<?php

declare(strict_types=1);

namespace Flux\Nats;

use Flux\InvalidCredentialsException;

/**
 * Credenciales NATS. NUNCA versionadas — 06-security.md §2. Se admite UN solo método:
 *
 * | Método | Campos |
 * |---|---|
 * | `.creds` (JWT + nkey), el recomendado | `creds` (ruta) o `credsContent` (contenido) |
 * | nkey | `nkeySeed` |
 * | token | `token` |
 * | usuario y contraseña | `user` + `pass` |
 *
 * `creds` es una **ruta** y se relee en cada (re)conexión: rotar no exige reiniciar. Todo lo que se puede comprobar sin red se
 * comprueba AQUÍ, antes de abrir ningún socket: un valor mal formado falla con `INVALID_CREDENTIALS` y su nombre, y **no** se degrada
 * a «sin credenciales» (que es lo que pasaba con una opción que el cliente ignoraba). Una credencial bien formada pero
 * equivocada es otra cosa —`CONNECTION_REJECTED`— y solo la decide el broker.
 */
final class Credentials
{
    private const KEYS = ['creds', 'credsContent', 'nkeySeed', 'token', 'user', 'pass'];

    /** @var array{seed:string,public:string}|null */
    private ?array $nkey = null;
    private ?string $jwt = null;

    private function __construct(
        private readonly ?string $credsPath,
        private readonly ?string $credsContent,
        private readonly ?string $nkeySeed,
        private readonly ?string $token,
        private readonly ?string $user,
        private readonly ?string $pass,
    ) {
    }

    /**
     * @param array<string,mixed>|self|null $options
     *
     * @throws InvalidCredentialsException
     */
    public static function from(array|self|null $options): ?self
    {
        if ($options instanceof self || $options === null) {
            return $options;
        }
        $unknown = array_values(array_diff(array_map('strval', array_keys($options)), self::KEYS));
        if ($unknown !== []) {
            // Un nombre equivocado (`password`, `credsFile`…) no debe degradar a «sin auth».
            throw new InvalidCredentialsException(
                'claves desconocidas: ' . implode(', ', $unknown) . '. Se aceptan: ' . implode(', ', self::KEYS)
            );
        }
        foreach ($options as $k => $v) {
            if (!is_string($v) || trim($v) === '') {
                throw new InvalidCredentialsException("`credentials.{$k}` debe ser un texto no vacío (casi siempre una variable de entorno sin definir)");
            }
        }
        if (isset($options['user']) !== isset($options['pass'])) {
            throw new InvalidCredentialsException('`user` y `pass` van juntos');
        }
        $methods = array_values(array_filter([
            isset($options['creds']) ? 'creds' : null,
            isset($options['credsContent']) ? 'credsContent' : null,
            isset($options['nkeySeed']) ? 'nkeySeed' : null,
            isset($options['token']) ? 'token' : null,
            isset($options['user']) ? 'user+pass' : null,
        ]));
        if ($methods === []) {
            return null;
        }
        if (count($methods) > 1) {
            // Combinarlos es ambiguo: el servidor aceptaría uno y el otro quedaría como ruido.
            throw new InvalidCredentialsException('elige UN método y no ' . implode(' + ', $methods));
        }

        $c = new self(
            $options['creds'] ?? null,
            $options['credsContent'] ?? null,
            $options['nkeySeed'] ?? null,
            $options['token'] ?? null,
            $options['user'] ?? null,
            $options['pass'] ?? null,
        );
        $c->validate($methods[0]);

        return $c;
    }

    /** Se prueba UNA vez, sin red, para que una semilla o un `.creds` malformados fallen aquí y no durante el handshake. */
    private function validate(string $method): void
    {
        if ($this->credsPath !== null && (!is_file($this->credsPath) || !is_readable($this->credsPath))) {
            throw new InvalidCredentialsException("`credentials.creds` apunta a \"{$this->credsPath}\", que no es un fichero legible");
        }
        if ($this->nkeySeed !== null && !str_starts_with(trim($this->nkeySeed), 'SU')) {
            throw new InvalidCredentialsException('`nkeySeed` debe ser la semilla de una nkey de USUARIO (empieza por «SU»)');
        }
        if (in_array($method, ['creds', 'credsContent', 'nkeySeed'], true)) {
            try {
                $this->resolve();
            } catch (\InvalidArgumentException $e) {
                throw new InvalidCredentialsException("{$method} no es utilizable: " . $e->getMessage(), $e);
            }
        }
    }

    /**
     * Resuelve la nkey (y el JWT de un `.creds`). Se relee el fichero en cada llamada: la rotación no exige reiniciar.
     *
     * @throws \InvalidArgumentException sin citar el valor de ninguna credencial
     */
    private function resolve(): void
    {
        if ($this->nkeySeed !== null) {
            $this->nkey = Nkeys::fromSeed($this->nkeySeed);
            $this->jwt = null;

            return;
        }
        $content = $this->credsContent ?? ($this->credsPath !== null ? @file_get_contents($this->credsPath) : null);
        if ($content === null || $content === false) {
            return;
        }
        $jwt = preg_match('/-----BEGIN NATS USER JWT-----\s*(\S+)\s*-----END NATS USER JWT-----/', $content, $m) === 1
            || preg_match('/-----BEGIN NATS USER JWT-----\s*(\S+)\s*------END NATS USER JWT------/', $content, $m) === 1 ? $m[1] : null;
        $seed = preg_match('/-----BEGIN USER NKEY SEED-----\s*(\S+)\s*-----END USER NKEY SEED-----/', $content, $s) === 1
            || preg_match('/-----BEGIN USER NKEY SEED-----\s*(\S+)\s*------END USER NKEY SEED------/', $content, $s) === 1 ? $s[1] : null;
        if ($jwt === null || $seed === null) {
            throw new \InvalidArgumentException('el .creds no contiene un JWT y una semilla de usuario (se esperan los bloques «NATS USER JWT» y «USER NKEY SEED»)');
        }
        $this->nkey = Nkeys::fromSeed($seed);
        $this->jwt = $jwt;
    }

    /**
     * Los campos de CONNECT para el nonce que dio el servidor en su INFO (o ninguno si no pidió firma).
     *
     * @return array<string,string>
     */
    public function connectFields(?string $nonce): array
    {
        if ($this->token !== null) {
            return ['auth_token' => $this->token];
        }
        if ($this->user !== null) {
            return ['user' => $this->user, 'pass' => (string) $this->pass];
        }
        $this->resolve(); // se relee: la rotación de un .creds no exige reiniciar
        if ($this->nkey === null) {
            return [];
        }
        $fields = ['nkey' => $this->nkey['public']];
        if ($this->jwt !== null) {
            $fields['jwt'] = $this->jwt;
        }
        if ($nonce !== null && $nonce !== '') {
            $fields['sig'] = Nkeys::sign($this->nkey['seed'], $nonce);
        }

        return $fields;
    }
}
