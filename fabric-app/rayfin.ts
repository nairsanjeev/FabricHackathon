import { createClient } from "@microsoft/rayfin-client";
import type { AppSchema } from "../../rayfin/data/schema";

const apiUrl =
  import.meta.env.VITE_RAYFIN_API_URL ?? "http://localhost:5168";
const publishableKey =
  import.meta.env.VITE_RAYFIN_PUBLISHABLE_KEY ?? "";

export const client = createClient<AppSchema>({
  apiUrl,
  publishableKey,
});
