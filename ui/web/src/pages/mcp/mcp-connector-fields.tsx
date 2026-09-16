import { useTranslation } from "react-i18next";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { UseFormReturn } from "react-hook-form";
import type { MCPFormData } from "@/schemas/mcp.schema";

interface McpConnectorFieldsProps {
  form: UseFormReturn<MCPFormData>;
  /** Editing an existing registration: the write-only key file is kept when left blank. */
  editing: boolean;
}

/**
 * ATH connector registration fields (settings.mode="ath-connector").
 * Operator/admin-only on the backend; the signer secret reference is
 * write-only — it never comes back from the API and is preserved on edit.
 */
export function McpConnectorFields({ form, editing }: McpConnectorFieldsProps) {
  const { t } = useTranslation("mcp");
  const { register, watch, setValue } = form;
  const enabled = watch("athConnector");
  const purpose = watch("athPurpose");

  if (!enabled) {
    return (
      <label className="flex items-center gap-2 text-sm">
        <input type="checkbox" {...register("athConnector")} className="h-4 w-4" />
        {t("form.connector.enable")}
      </label>
    );
  }

  return (
    <div className="grid gap-3 rounded-md border border-dashed border-border p-3">
      <label className="flex items-center gap-2 text-sm font-medium">
        <input type="checkbox" {...register("athConnector")} className="h-4 w-4" />
        {t("form.connector.enable")}
      </label>

      <div className="grid gap-1.5">
        <Label htmlFor="ath-gateway-url">{t("form.connector.gatewayUrl")}</Label>
        <Input id="ath-gateway-url" placeholder="https://ath.example.test" {...register("athGatewayUrl")} />
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor="ath-connector-id">{t("form.connector.connectorId")}</Label>
          <Input id="ath-connector-id" placeholder="00000000-0000-0000-0000-000000000000" {...register("athConnectorId")} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="ath-environment">{t("form.connector.environment")}</Label>
          <Input id="ath-environment" placeholder="production" {...register("athEnvironment")} />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor="ath-issuer">{t("form.connector.issuer")}</Label>
          <Input id="ath-issuer" placeholder="goclaw-ath" {...register("athIssuer")} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="ath-key-id">{t("form.connector.keyId")}</Label>
          <Input id="ath-key-id" placeholder="key-1" {...register("athKeyId")} />
        </div>
      </div>

      <div className="grid gap-1.5">
        <Label htmlFor="ath-private-key">{t("form.connector.privateKeyFile")}</Label>
        <Input
          id="ath-private-key"
          type="password"
          placeholder={editing ? t("form.connector.privateKeyUnchanged") : "/run/secrets/ath-ed25519"}
          {...register("athPrivateKeyFile")}
        />
        <p className="text-xs text-muted-foreground">{t("form.connector.privateKeyHint")}</p>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor="ath-channel-instance">{t("form.connector.channelInstanceId")}</Label>
          <Input id="ath-channel-instance" placeholder="00000000-0000-0000-0000-000000000000" {...register("athChannelInstanceId")} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="ath-account-id">{t("form.connector.accountId")}</Label>
          <Input id="ath-account-id" placeholder="00000000-0000-0000-0000-000000000000" {...register("athAccountId")} />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div className="grid gap-1.5">
          <Label htmlFor="ath-provider-account">{t("form.connector.providerAccountId")}</Label>
          <Input id="ath-provider-account" placeholder="account-1" {...register("athProviderAccountId")} />
        </div>
        <div className="grid gap-1.5">
          <Label htmlFor="ath-account-epoch">{t("form.connector.accountEpoch")}</Label>
          <Input id="ath-account-epoch" type="number" min={1} placeholder="1" {...register("athAccountEpoch")} />
        </div>
        <div className="grid gap-1.5">
          <Label>{t("form.connector.purpose")}</Label>
          <Select value={purpose} onValueChange={(v) => setValue("athPurpose", v as MCPFormData["athPurpose"])}>
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="tenant_contract">{t("form.connector.purposes.tenantContract")}</SelectItem>
              <SelectItem value="sales_inventory">{t("form.connector.purposes.salesInventory")}</SelectItem>
              <SelectItem value="management_access">{t("form.connector.purposes.managementAccess")}</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <p className="text-xs text-muted-foreground">{t("form.connector.trustHint")}</p>
    </div>
  );
}
