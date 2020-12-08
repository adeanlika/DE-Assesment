CREATE INDEX idx_campaign_url ON public.campaign USING btree (url);
CREATE INDEX idx_ads_url ON public.ads_spent USING btree (short_url);
CREATE INDEX idx_visit_url ON public.visit USING btree (campaign_url);