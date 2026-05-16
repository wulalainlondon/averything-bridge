from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from bridge_v2 import Session


class Backend(ABC):
    """AI backend 抽象介面。所有 backend 必須實作這四個方法。"""

    @abstractmethod
    async def spawn(self, session: "Session") -> None:
        """建立 AI 連線 / 啟動子行程。session 建立時呼叫。"""

    @abstractmethod
    async def send(self, session: "Session", content: str,
                   images: list | None = None, files: list | None = None) -> None:
        """送出使用者訊息，開始串流回應。"""

    @abstractmethod
    async def stop(self, session: "Session") -> None:
        """中止目前正在進行的回應。"""

    @abstractmethod
    async def clear(self, session: "Session") -> None:
        """清除歷史，重置 session 狀態。"""

    async def close(self, session: "Session") -> None:
        """關閉 session，釋放資源。預設呼叫 stop。"""
        await self.stop(session)

    def supports_resume(self) -> bool:
        """此 backend 是否支援 session resume。預設不支援。"""
        return False

    async def fetch_usage(self, ws: Any) -> None:
        """送出用量報告到 WebSocket。預設無作為（不支援的 backend）。"""
        pass

    async def get_resumable_sessions(self, limit: int = 100) -> list[dict]:
        """回傳可恢復的 session 列表。預設回傳空列表。"""
        return []

    async def load_session_history(
        self,
        resume_id: str,
        limit: int = 120,
        known_last_source_message_id: str = "",
        mode: str = "snapshot",
        before_source_message_id: str = "",
    ) -> list[dict] | dict:
        """載入 session 歷史紀錄。預設回傳空列表。"""
        return []
