import { createFileRoute } from '@tanstack/react-router'
import * as React from 'react'

export const Route = createFileRoute('/')({
  component: Dashboard,
})

function Dashboard() {
  const [metrics, setMetrics] = React.useState<any[]>([])

  React.useEffect(() => {
    // Connect to SSE endpoint
    // For now hardcode pipeline ID 'p1'
    const eventSource = new EventSource('/api/v1/pipelines/p1/metrics')

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data)
      setMetrics((prev) => [...prev.slice(-19), data])
    }

    eventSource.onerror = (err) => {
      console.error('SSE failed:', err)
      eventSource.close()
    }

    return () => {
      eventSource.close()
    }
  }, [])

  return (
    <div className="p-4">
      <h1 className="text-2xl font-bold mb-4">Daya Data Pipeline Dashboard</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="border p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">Real-time Events</h2>
          <div className="h-64 overflow-y-auto bg-gray-100 p-2 font-mono text-sm">
            {metrics.length === 0 && <p className="text-gray-500">Waiting for data...</p>}
            {metrics.map((m, i) => (
              <div key={i} className="mb-1 border-b pb-1">
                <span className="text-blue-600">[{m.key}]</span> LSN: {m.checkpoint.i_lsn || m.checkpoint.e_lsn} Status: {m.checkpoint.status}
              </div>
            ))}
          </div>
        </div>
        <div className="border p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">System Status</h2>
          <div className="h-64 flex flex-col items-center justify-center bg-gray-100">
            <div className="text-4xl font-bold text-green-600">ONLINE</div>
            <p className="text-gray-500 mt-2">NATS JetStream: Connected</p>
            <p className="text-gray-500">Postgres CDC: Active</p>
          </div>
        </div>
      </div>
      
      <div className="mt-8 border p-4 rounded shadow">
        <h2 className="text-lg font-semibold mb-4">Active Pipelines</h2>
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Last Sync</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
             <tr>
               <td className="px-6 py-4 whitespace-nowrap">p1</td>
               <td className="px-6 py-4 whitespace-nowrap">
                 <span className="px-2 py-1 bg-green-100 text-green-800 rounded-full text-xs">RUNNING</span>
               </td>
               <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">Just now</td>
             </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}
