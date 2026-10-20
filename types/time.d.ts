interface Window {
  VIREO_TIME_FORMAT?: 'system' | '12h' | '24h';
  VireoTime: {
    setFormat(value: unknown): void;
    time(value: Date, options?: Intl.DateTimeFormatOptions): string;
    dateTime(value: Date, options?: Intl.DateTimeFormatOptions): string;
    captureTime(value: string | null | undefined, seconds?: boolean): string;
    captureDateTime(value: string | null | undefined, seconds?: boolean, options?: Intl.DateTimeFormatOptions): string;
  };
}
