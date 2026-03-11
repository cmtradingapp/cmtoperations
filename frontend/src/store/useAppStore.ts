import { create } from 'zustand';

import type { CallStatusType, ClientDetail, FilterParams } from '../types';

interface AppState {
  // Filters
  filters: FilterParams;
  setFilters: (patch: Partial<FilterParams>) => void;
  resetFilters: () => void;

  // Search results
  results: ClientDetail[];
  setResults: (results: ClientDetail[]) => void;
  isSearching: boolean;
  setIsSearching: (v: boolean) => void;
  searchError: string | null;
  setSearchError: (e: string | null) => void;

  // Row selection
  selectedIds: Set<string>;
  toggleSelected: (id: string) => void;
  selectAll: () => void;
  deselectAll: () => void;

  // Per-client call statuses
  callStatuses: Record<string, CallStatusType>;
  setCallStatus: (clientId: string, status: CallStatusType) => void;
  resetCallStatuses: () => void;
  isCalling: boolean;
  setIsCalling: (v: boolean) => void;

  // Per-client conversation IDs (set after successful call)
  conversationIds: Record<string, string>;
  setConversationId: (clientId: string, convId: string) => void;

  // Agent settings
  agentId: string;
  setAgentId: (v: string) => void;
  agentPhoneNumberId: string;
  setAgentPhoneNumberId: (v: string) => void;
}

const defaultFilters: FilterParams = {};

export const useAppStore = create<AppState>((set) => ({
  // Filters
  filters: defaultFilters,
  setFilters: (patch) =>
    set((state) => ({ filters: { ...state.filters, ...patch } })),
  resetFilters: () => set({ filters: defaultFilters }),

  // Results — also clears selection and call statuses when new results arrive
  results: [],
  setResults: (results) =>
    set({ results, selectedIds: new Set(), callStatuses: {}, conversationIds: {} }),
  isSearching: false,
  setIsSearching: (v) => set({ isSearching: v }),
  searchError: null,
  setSearchError: (e) => set({ searchError: e }),

  // Selection
  selectedIds: new Set(),
  toggleSelected: (id) =>
    set((state) => {
      const next = new Set(state.selectedIds);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return { selectedIds: next };
    }),
  selectAll: () =>
    set((state) => ({
      // Only select clients that have not been called yet
      selectedIds: new Set(
        state.results
          .filter((r) => !state.conversationIds[r.client_id])
          .map((r) => r.client_id)
      ),
    })),
  deselectAll: () => set({ selectedIds: new Set() }),

  // Call statuses
  callStatuses: {},
  setCallStatus: (clientId, status) =>
    set((state) => ({
      callStatuses: { ...state.callStatuses, [clientId]: status },
    })),
  resetCallStatuses: () => set({ callStatuses: {} }),
  isCalling: false,
  setIsCalling: (v) => set({ isCalling: v }),

  // Conversation IDs — also moves the called client to the bottom of results
  conversationIds: {},
  setConversationId: (clientId, convId) =>
    set((state) => {
      const newResults = [...state.results];
      const idx = newResults.findIndex((r) => r.client_id === clientId);
      if (idx !== -1) {
        const [called] = newResults.splice(idx, 1);
        newResults.push(called);
      }
      return {
        conversationIds: { ...state.conversationIds, [clientId]: convId },
        results: newResults,
        selectedIds: (() => {
          const next = new Set(state.selectedIds);
          next.delete(clientId);
          return next;
        })(),
      };
    }),

  // Agent settings (pre-filled from env defaults, editable in UI)
  agentId: '',
  setAgentId: (v) => set({ agentId: v }),
  agentPhoneNumberId: '',
  setAgentPhoneNumberId: (v) => set({ agentPhoneNumberId: v }),
}));
