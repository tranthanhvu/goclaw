/**
 * Gateway HTTP API base URL for separately-hosted SPA deployments.
 *
 * Empty (default) = same-origin: the UI is served by the gateway binary or its
 * bundled nginx image, so relative paths like /v1/... resolve correctly.
 *
 * Set VITE_API_URL (e.g. https://gw.example.com) when the SPA is hosted on a
 * different origin — e.g. Vercel — so HTTP calls and file/media URLs resolve
 * against the gateway. The gateway must list the SPA origin in
 * gateway.allowed_origins (WS handshake + CORS).
 */
export const API_BASE = ((import.meta.env.VITE_API_URL as string | undefined) ?? "").replace(
  /\/+$/,
  "",
);

/** Prefix a relative gateway path (e.g. /v1/files/x) with API_BASE.
 *  Absolute URLs (http/https/blob/data) pass through unchanged. */
export function apiUrl(path: string): string {
  if (!API_BASE || !path) return path;
  if (/^(https?:|blob:|data:)/i.test(path)) return path;
  return API_BASE + (path.startsWith("/") ? path : `/${path}`);
}
