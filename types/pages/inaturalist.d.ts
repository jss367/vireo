interface CandidatePhoto {id: number; filename: string}
interface Window {
  photoThumbnailUrl(photo: CandidatePhoto): string;
  openLightbox(id: number, filename: string, photos: CandidatePhoto[]): void;
  openExternalLink(event: MouseEvent, url: string): void;
  safeFetch<T>(url: string): Promise<T>;
  submitToInat(id: number, comparison: {login: string; name: string; id: number}): Promise<unknown>;
}
