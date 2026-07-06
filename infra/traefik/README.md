# Traefik — enrutado por labels, TLS automático y middlewares

Traefik es el **único punto de entrada** del stack. Termina TLS, redirige
HTTP→HTTPS, enruta a los servicios por labels y aplica middlewares de
seguridad y rate limiting a la API.

## Archivos

| Archivo | Rol |
| --- | --- |
| `traefik.yml` | Config estática: entrypoints, providers, resolver ACME. |
| `dynamic/middlewares.yml` | Middlewares: headers, compresión, rate limit, strip; chains `web-chain`/`api-chain`. |
| `dynamic/tls.yml` | Opciones TLS (min 1.2, ciphers AEAD, `sniStrict`). |
| `docker-compose.yml` | Traefik + servicios `web`/`api` con routers por labels. |

## Enrutado (por labels)

- **web** (SPA): `PathPrefix(/)`, prioridad `1` → captura todo lo no-API.
- **api**: `PathPrefix(/api)`, prioridad `100` → gana sobre `web`; `api-strip`
  quita el prefijo `/api` antes de llegar al backend.

Ambos routers escuchan en `websecure` (:443) con `certresolver=letsencrypt`.

## TLS automático

- Entrypoint `web` (:80) redirige de forma permanente a `websecure` (:443).
- El resolver `letsencrypt` usa **HTTP-01** sobre `:80` por defecto.
- `acme.json` vive en el volumen `acme` → **renovación automática** sin
  re-emisión ni pérdida de certificados entre reinicios.
- Alternativa **DNS-01** (wildcards / sin exponer :80) documentada y
  comentada en `traefik.yml`.

## Middlewares en la API

`api-chain` aplica, en orden: `api-strip` → `security-headers` →
`compression` → `api-ratelimit` (**50 req/s**, ráfaga **100**).
`web-chain` aplica `security-headers` + `compression`.

## Uso

```bash
export ACME_EMAIL="ops@ejemplo.com"
export APP_HOST="app.ejemplo.com"
# Opcional: WEB_IMAGE / API_IMAGE apuntando a las imágenes reales.
docker compose -f infra/traefik/docker-compose.yml up -d
```

> Los servicios `web`/`api` usan `traefik/whoami` como placeholder; en el
> stack real se reemplazan por las imágenes construidas conservando labels.
