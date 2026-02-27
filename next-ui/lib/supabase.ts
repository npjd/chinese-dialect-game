const supabaseUrl = (process.env.NEXT_PUBLIC_SUPABASE_URL || "").replace(/\/+$/, "");

export const hasSupabaseConfig = Boolean(supabaseUrl);
export function getPublicStorageUrl(objectPath: string) {
  const encodedPath = objectPath
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return `${supabaseUrl}/storage/v1/object/public/${bucket}/${encodedPath}`;
}

export const bucket = process.env.NEXT_PUBLIC_SUPABASE_BUCKET || "dialect-game";
export const indexObjectPath =
  process.env.NEXT_PUBLIC_SUPABASE_INDEX_OBJECT_PATH || "indexes/all_clips.json";
