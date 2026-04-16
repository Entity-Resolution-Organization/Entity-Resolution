import Plotly from 'plotly.js/dist/plotly';
import factory from 'react-plotly.js/factory';

// factory might be { default: fn } or fn depending on Vite's CJS interop
const createPlot = typeof factory === 'function' ? factory : factory.default;
const Plot = createPlot(Plotly);

export default Plot;
