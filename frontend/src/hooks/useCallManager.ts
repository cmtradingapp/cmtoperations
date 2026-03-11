import { initiateCalls } from '../api/client';
import { useAppStore } from '../store/useAppStore';

export function useCallManager() {
  const {
    selectedIds,
    conversationIds,
    agentId,
    agentPhoneNumberId,
    setCallStatus,
    setIsCalling,
    setConversationId,
  } = useAppStore();

  const callSelected = async () => {
    // Skip any client that already has a conversation_id
    const ids = Array.from(selectedIds).filter((id) => !conversationIds[id]);
    if (ids.length === 0) return;

    setIsCalling(true);
    ids.forEach((id) => setCallStatus(id, 'calling'));

    try {
      const response = await initiateCalls(ids, agentId, agentPhoneNumberId);
      response.results.forEach((r) => {
        if (r.status === 'initiated' && r.conversation_id) {
          setConversationId(r.client_id, r.conversation_id);
        }
        setCallStatus(r.client_id, r.status === 'initiated' ? 'initiated' : 'failed');
      });
    } catch {
      ids.forEach((id) => setCallStatus(id, 'failed'));
    } finally {
      setIsCalling(false);
    }
  };

  return { callSelected };
}
