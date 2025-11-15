const API_URL = "http://localhost:8000"; // адрес FastAPI backend

export async function sendPrompt(prompt) {
  const res = await fetch(`${API_URL}/ask`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ prompt }),
  });

  if (!res.ok) throw new Error("Failed");

  return await res.json(); // формат должен быть: { type: "text"/"table", ... }
}
