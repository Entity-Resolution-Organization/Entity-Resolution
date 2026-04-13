import { lazy, Suspense, memo } from 'react';
const Plot = lazy(() => import('react-plotly.js'));

const COLORS = { MATCH: '#059669', REVIEW: '#b45309', 'NO-MATCH': '#dc2626' };

function ProbGauge({ probability, decision }) {
  const color = COLORS[decision] || '#6b7280';
  return (
    <Suspense fallback={<div className="h-[200px] skeleton" />}>
    <Plot
      data={[{
        type: 'indicator',
        mode: 'gauge+number',
        value: probability * 100,
        number: { suffix: '%', font: { size: 42, color: '#1e293b', family: 'Outfit, system-ui' } },
        gauge: {
          axis: {
            range: [0, 100],
            tickcolor: 'rgba(0,0,0,0.08)',
            tickfont: { color: '#64748b', size: 10 },
            tickwidth: 1,
          },
          bar: { color, thickness: 0.55 },
          bgcolor: 'rgba(0,0,0,0)',
          borderwidth: 0,
          steps: [
            { range: [0, 20], color: 'rgba(220,38,38,0.06)' },
            { range: [20, 45], color: 'rgba(180,83,9,0.06)' },
            { range: [45, 100], color: 'rgba(5,150,105,0.06)' },
          ],
          threshold: {
            line: { color, width: 3 },
            thickness: 0.8,
            value: probability * 100,
          },
        },
      }]}
      layout={{
        height: 200,
        margin: { t: 24, b: 0, l: 24, r: 24 },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        font: { color: '#64748b', family: 'Outfit, system-ui' },
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
    </Suspense>
  );
}

export default memo(ProbGauge);
