/*
 * TLS del listener — 06-security.md §2: es OBLIGATORIO en produccion.
 */
package com.flux;

/**
 * TLS de la conexion a NATS.
 *
 * <p>Los ficheros son RUTAS a ficheros PEM. {@code new TlsOptions()} (sin nada) EXIGE TLS y
 * verifica contra las CA del sistema (el almacen de confianza de la JVM). Un bus interno casi
 * nunca usa una CA publica: {@link #caFile(String)} fija la propia, y es lo que evita acabar con
 * la verificacion desactivada, que es la forma mas corriente de tener TLS «obligatorio» y sin
 * proteccion.
 *
 * <p>El SDK SIEMPRE verifica que el certificado del servidor cubre el host al que se conecta
 * (los SAN): la libreria de NATS para Java no lo hace por si sola (medido: conecta a un host que
 * el certificado no cubre), asi que el SDK se lo impone.
 *
 * <pre>{@code
 * new FluxBus.ConnectOptions()
 *         .servers("tls://nats.interno:4222")
 *         .tls(new TlsOptions().caFile("/etc/flux/ca.pem"));
 * }</pre>
 */
public final class TlsOptions {
    private String caFile;
    private String certFile;
    private String keyFile;

    /** CA que firma el certificado del servidor NATS, si no es una CA publica. Un PEM con uno o mas certificados. */
    public TlsOptions caFile(String path) {
        this.caFile = path;
        return this;
    }

    /** Certificado de cliente (mTLS), en PEM. Exige {@link #keyFile(String)}. */
    public TlsOptions certFile(String path) {
        this.certFile = path;
        return this;
    }

    /** Clave del certificado de cliente, en PEM (PKCS#8, PKCS#1 o SEC1). Exige {@link #certFile(String)}. */
    public TlsOptions keyFile(String path) {
        this.keyFile = path;
        return this;
    }

    String caFile() {
        return caFile;
    }

    String certFile() {
        return certFile;
    }

    String keyFile() {
        return keyFile;
    }
}
