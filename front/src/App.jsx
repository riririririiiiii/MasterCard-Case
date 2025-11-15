import { useState } from "react";
import ChatInput from "./components/ChatInput";
import ResponseBox from "./components/ResponseBox";
import { sendPrompt } from "./services/api";
import './styles/styles.css';

function App() {
  const [loading, setLoading] = useState(false);
  const [response, setResponse] = useState(null);
  const [error, setError] = useState("");

  const handlePromptSubmit = async (prompt) => {
    setLoading(true);
    setError("");
    try {
      const result = await sendPrompt(prompt);
      setResponse(result);
    } catch (e) {
      setError("❌ Error fetching data");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app-container">
      <h1>Agentic Analyst</h1>
      <ChatInput onSubmit={handlePromptSubmit} />
      {loading && <p>⏳ Loading...</p>}
      {error && <p style={{ color: "red" }}>{error}</p>}
      {response && <ResponseBox data={response} />}
    </div>
  );
}

export default App;