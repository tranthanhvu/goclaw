import { defineConfig, loadEnv, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import path from "path";

/**
 * When VITE_API_URL is set (SPA hosted separately from the gateway, e.g. on
 * Vercel), inject the gateway origin into the CSP connect-src directive in
 * index.html. The base CSP is same-origin only; without this the browser
 * blocks cross-origin /v1 calls. No-op when VITE_API_URL is unset so the
 * default same-origin deployment keeps its strict policy.
 */
function cspGatewayOrigin(apiBaseUrl: string): Plugin {
  let origin = "";
  try {
    if (apiBaseUrl) origin = new URL(apiBaseUrl).origin;
  } catch {
    // Invalid VITE_API_URL — leave CSP unchanged; apiUrl() will surface the error.
  }
  if (!origin) return { name: "csp-gateway-origin" };
  return {
    name: "csp-gateway-origin",
    transformIndexHtml(html: string) {
      return html.replace(
        "connect-src 'self' ws: wss:;",
        `connect-src 'self' ${origin} ws: wss:;`,
      );
    },
  };
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const backendPort = env.VITE_BACKEND_PORT || "9600";
  const backendHost = env.VITE_BACKEND_HOST || "localhost";

  return {
    plugins: [react(), tailwindcss(), cspGatewayOrigin(env.VITE_API_URL || "")],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 5173,
      proxy: {
        "/ws": {
          target: `http://${backendHost}:${backendPort}`,
          ws: true,
          changeOrigin: true,
        },
        "/v1": {
          target: `http://${backendHost}:${backendPort}`,
          changeOrigin: true,
          timeout: 30000, // 30s for large audio responses
        },
        "/health": {
          target: `http://${backendHost}:${backendPort}`,
          changeOrigin: true,
        },
      },
    },
    build: {
      outDir: "dist",
      emptyOutDir: true,
    },
  };
});
