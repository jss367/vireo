interface CandidatePhoto {id: number; filename: string; timestamp?: string; location_name?: string}
interface Window {
  photoThumbnailUrl(photo: CandidatePhoto): string;
  openLightbox(id: number, filename: string, photos: CandidatePhoto[]): void;
  openExternalLink(event: MouseEvent, url: string): void;
  safeFetch<T>(url: string, options?: RequestInit): Promise<T>;
  submitToInat(id: number, comparison: {login: string; name: string; id: number; additionalPhotoIds?: number[]}): Promise<unknown>;
}
