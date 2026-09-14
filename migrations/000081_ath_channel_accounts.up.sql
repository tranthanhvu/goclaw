-- Stable account identity per channel instance: one row per
-- (tenant, channel instance, provider). account_epoch rises whenever a
-- different provider account logs in on the same instance, invalidating
-- contexts and assertions minted under the previous account.
CREATE TABLE IF NOT EXISTS ath_channel_accounts (
    id                  UUID PRIMARY KEY,
    tenant_id           UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    channel_instance_id UUID NOT NULL REFERENCES channel_instances(id) ON DELETE CASCADE,
    provider            VARCHAR(40) NOT NULL,
    provider_account_id VARCHAR(300) NOT NULL,
    account_epoch       INTEGER NOT NULL DEFAULT 1 CHECK (account_epoch >= 1),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_instance_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_ath_channel_accounts_tenant ON ath_channel_accounts(tenant_id);
CREATE INDEX IF NOT EXISTS idx_ath_channel_accounts_instance ON ath_channel_accounts(channel_instance_id);
