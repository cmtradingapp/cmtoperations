import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';

import App from './App';
import './index.css';

// Apply saved theme class before first paint to prevent flash
try {
  const saved = localStorage.getItem('theme');
  if (saved) {
    const parsed = JSON.parse(saved);
    if (parsed?.state?.isDark) {
      document.documentElement.classList.add('dark');
    }
  }
} catch {
  // ignore parse errors
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>
);
