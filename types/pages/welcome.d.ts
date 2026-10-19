// The Welcome (first-run setup) page (vireo/static/welcome/): the model
// status, place search and job payloads it reads. It has no navbar and reads
// no shared globals.
interface WelcomeModelStatus {
  available_models?: {id: string; downloaded?: boolean}[];
  classification?: {labels_ready?: boolean} | null;
}
interface WelcomePlace {id: number | string; display_name?: string; name?: string}
interface WelcomeJobProgress {current: number; total: number; phase?: string; current_file?: string}
interface WelcomeJobResult {
  status: string; errors?: string[];
  result?: {species_count?: number; embedding_precompute?: {model_id?: string; labels_file?: string} | null};
}
