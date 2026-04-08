import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || '',
  timeout: 30000,
  headers: { 'Content-Type': 'application/json' },
});

export const resolveEntities = (pair) => api.post('/resolve', pair);
export const resolveBatch = (pairs) => api.post('/resolve/batch', { pairs });
export const searchEntity = (name, address, topK = 10) =>
  api.post('/search', { name, address, top_k: topK });
export const getHealth = () => api.get('/health');
export const getPipelineMetrics = () => api.get('/metrics/pipeline');
export const getInferenceMetrics = () => api.get('/metrics/inference');

export default api;
