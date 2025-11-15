import { useState } from "react";
import '../styles/styles.css';

export default function ChatInput({ onSubmit }) {
  const [prompt, setPrompt] = useState("");

  const handleSubmit = (e) => {
    e.preventDefault();
    if (prompt.trim()) {
      onSubmit(prompt);
      setPrompt("");
    }
  };

  return (
    <form onSubmit={handleSubmit}>
      <input
        type="text"
        placeholder="Ask something like: Total revenue for SilkPay in 2024"
        value={prompt}
        onChange={(e) => setPrompt(e.target.value)}
        style={{ width: "400px", padding: "10px" }}
      />
      <button type="submit">Submit</button>
    </form>
  );
}