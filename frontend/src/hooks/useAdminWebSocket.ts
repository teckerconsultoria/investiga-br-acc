import { useCallback, useEffect, useRef, useState } from "react";
interface LogEntry {
  type: string;
  source?: string;
  line?: string;
  run_id?: string;
  status?: string;
  exit_code?: number;
  message?: string;
  cmd?: string;
}

export function useAdminWebSocket(
  neo4jPassword: string,
) {
  const wsRef = useRef<WebSocket | null>(null);
  const pendingMessageRef = useRef<string | null>(null);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isRunning, setIsRunning] = useState(false);
  const onEndRef = useRef<((status: string) => void) | null>(null);

  const connect = useCallback(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const host = window.location.host;
    const ws = new WebSocket(`${protocol}//${host}/api/v1/admin/ws/logs`);

    ws.onopen = () => {
      setIsConnected(true);
      const pending = pendingMessageRef.current;
      if (pending) {
        pendingMessageRef.current = null;
        ws.send(pending);
        setIsRunning(true);
      }
    };
    ws.onclose = () => {
      setIsConnected(false);
      wsRef.current = null;
    };
    ws.onerror = () => {
      setIsConnected(false);
    };
    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        setLogs((prev) => [...prev, data]);
        if (data.type === "end" || data.type === "error") {
          setIsRunning(false);
          onEndRef.current?.(data.status || "error");
        }
      } catch {
        setLogs((prev) => [...prev, { type: "log", source: "system", line: event.data }]);
      }
    };

    wsRef.current = ws;
  }, []);

  const disconnect = useCallback(() => {
    wsRef.current?.close();
    wsRef.current = null;
    setIsConnected(false);
    setIsRunning(false);
  }, []);

  const runPipeline = useCallback(
    (pipelineId: string) => {
      const msg = JSON.stringify({
        type: "pipeline",
        pipeline_id: pipelineId,
        neo4j_password: neo4jPassword,
      });
      setLogs([]);
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
        pendingMessageRef.current = msg;
        connect();
      } else {
        wsRef.current.send(msg);
        setIsRunning(true);
      }
    },
    [connect, neo4jPassword],
  );

  const runBootstrap = useCallback(
    (sources: string, resetDb: boolean) => {
      const msg = JSON.stringify({
        type: "bootstrap",
        sources,
        reset_db: resetDb,
        neo4j_password: neo4jPassword,
      });
      setLogs([]);
      if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) {
        pendingMessageRef.current = msg;
        connect();
      } else {
        wsRef.current.send(msg);
        setIsRunning(true);
      }
    },
    [connect, neo4jPassword],
  );

  const onEnd = useCallback((cb: (status: string) => void) => {
    onEndRef.current = cb;
  }, []);

  useEffect(() => {
    return () => {
      wsRef.current?.close();
    };
  }, []);

  return {
    logs,
    isConnected,
    isRunning,
    connect,
    disconnect,
    runPipeline,
    runBootstrap,
    onEnd,
  };
}
