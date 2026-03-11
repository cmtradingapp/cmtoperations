import { ClientTable } from '../components/ClientTable';
import { FilterPanel } from '../components/FilterPanel';

export function CallManagerPage() {
  return (
    <>
      <FilterPanel />
      <ClientTable />
    </>
  );
}
