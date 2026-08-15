import { useQuery } from "@tanstack/react-query";
import { relayHttpBaseUrl } from "@/shared/lib/relay-url";

export interface RelayInfo {
  contact?: string;
}

async function fetchRelayInfo(): Promise<RelayInfo> {
  const response = await fetch(relayHttpBaseUrl(), {
    headers: { Accept: "application/nostr+json" },
  });
  if (!response.ok) throw new Error("Could not load relay information.");
  return response.json() as Promise<RelayInfo>;
}

export function useRelayInfo() {
  return useQuery({
    queryKey: ["relay-info"],
    queryFn: fetchRelayInfo,
    staleTime: 5 * 60_000,
    retry: 1,
  });
}
