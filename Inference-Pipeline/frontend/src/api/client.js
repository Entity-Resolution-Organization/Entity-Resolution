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

// Unify pipeline (CSV upload -> graph -> clusters -> download)
export const uploadUnify = (file) => {
  const form = new FormData();
  form.append('file', file);
  return api.post('/unify/upload', form, {
    headers: { 'Content-Type': 'multipart/form-data' },
    timeout: 60000,
  });
};
export const getUnifyStatus = (jobId) => api.get(`/unify/status/${jobId}`);
export const getUnifyJobs = () => api.get('/unify/jobs');
export const downloadUnified = (jobId) =>
  api.get(`/unify/download/${jobId}`, { responseType: 'blob' });

export default api;
