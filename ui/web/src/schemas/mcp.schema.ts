import { z } from "zod";

export const mcpFormSchema = z.object({
  name: z.string().min(1),
  displayName: z.string(),
  transport: z.enum(["stdio", "sse", "streamable-http"]),
  command: z.string(),
  args: z.string(),
  url: z.string(),
  headers: z.record(z.string(), z.string()),
  env: z.record(z.string(), z.string()),
  toolPrefix: z.string(),
  timeout: z.number().min(1),
  enabled: z.boolean(),
  requireUserCreds: z.boolean(),
  // Admin-authored description hints appended to MCP tool descriptions so the
  // LLM sees server-specific quirks. Persisted under settings.tool_hints.
  toolHintsGlobal: z.string(),
  toolHintsTools: z.record(z.string(), z.string()),
  // ATH connector registration (settings.mode="ath-connector"). Required as a
  // group when the toggle is on — enforced in the form submit, mirroring the
  // backend enablement contract.
  athConnector: z.boolean(),
  athGatewayUrl: z.string(),
  athConnectorId: z.string(),
  athEnvironment: z.string(),
  athIssuer: z.string(),
  athKeyId: z.string(),
  athPrivateKeyFile: z.string(),
  athChannelInstanceId: z.string(),
  athPurpose: z.enum(["tenant_contract", "sales_inventory", "management_access"]),
});

export type MCPFormData = z.infer<typeof mcpFormSchema>;
