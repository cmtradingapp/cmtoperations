import { getClients } from '../api/client';
import { useAppStore } from '../store/useAppStore';

export function useClientSearch() {
  const { filters, setResults, setIsSearching, setSearchError } = useAppStore();

  const search = async () => {
    setIsSearching(true);
    setSearchError(null);
    try {
      const data = await getClients(filters);
      setResults(data);
    } catch (err: unknown) {
      const msg =
        (err as { response?: { data?: { detail?: string } }; message?: string })
          ?.response?.data?.detail ??
        (err as { message?: string })?.message ??
        'Search failed';
      setSearchError(msg);
    } finally {
      setIsSearching(false);
    }
  };

  return { search };
}
