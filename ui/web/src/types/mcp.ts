export interface MCPServerData {
  id: string;
  name: string;
  display_name: string;
  transport: "stdio" | "sse" | "streamable-http";
  command: string;
  args: string[] | null;
  url: string;
  headers: Record<string, string> | null;
  env: Record<string, string> | null;
  tool_prefix: string;
  timeout_sec: number;
  settings?: {
    require_user_credentials?: boolean;
    tool_hints?: {
      global?: string;
      tools?: Record<string, string>;
    };
    /** ATH connector registration (mode="ath-connector"). private_key_file is
     *  write-only: never present in API responses; omit on update to keep it. */
    mode?: "ath-connector";
    gateway_url?: string;
    connector_id?: string;
    environment?: string;
    issuer?: string;
    key_id?: string;
    private_key_file?: string;
    channel_instance_id?: string;
    account_id?: string;
    account_epoch?: number;
    provider_account_id?: string;
    purpose?: "tenant_contract" | "sales_inventory" | "management_access";
  };
  enabled: boolean;
  created_by: string;
  agent_count?: number;
  created_at: string;
  updated_at: string;
}

export interface MCPServerInput {
  name: string;
  display_name?: string;
  transport: string;
  command?: string;
  args?: string[];
  url?: string;
  headers?: Record<string, string>;
  env?: Record<string, string>;
  tool_prefix?: string;
  timeout_sec?: number;
  settings?: {
    require_user_credentials?: boolean;
    tool_hints?: {
      global?: string;
      tools?: Record<string, string>;
    };
    /** ATH connector registration (mode="ath-connector"). private_key_file is
     *  write-only: never present in API responses; omit on update to keep it. */
    mode?: "ath-connector";
    gateway_url?: string;
    connector_id?: string;
    environment?: string;
    issuer?: string;
    key_id?: string;
    private_key_file?: string;
    channel_instance_id?: string;
    account_id?: string;
    account_epoch?: number;
    provider_account_id?: string;
    purpose?: "tenant_contract" | "sales_inventory" | "management_access";
  };
  enabled?: boolean;
}

export interface MCPToolInfo {
  name: string;
  description?: string;
}

export interface MCPAgentGrant {
  id: string;
  server_id: string;
  agent_id: string;
  enabled: boolean;
  tool_allow: string[] | null;
  tool_deny: string[] | null;
  granted_by: string;
  created_at: string;
}

export interface MCPUserCredentialStatus {
  user_id?: string;
  has_credentials: boolean;
  has_api_key: boolean;
  has_headers: boolean;
  has_env: boolean;
}

export interface MCPUserCredentialInput {
  api_key?: string;
  headers?: Record<string, string>;
  env?: Record<string, string>;
}
