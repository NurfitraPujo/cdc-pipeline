import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/')({
  component: Dashboard,
})

function Dashboard() {
  return (
    <div className="p-4">
      <h1 className="text-2xl font-bold mb-4">Daya Data Pipeline Dashboard</h1>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="border p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">Throughput (msgs/sec)</h2>
          <div className="h-64 flex items-center justify-center bg-gray-100">
            [Throughput Chart Placeholder]
          </div>
        </div>
        <div className="border p-4 rounded shadow">
          <h2 className="text-lg font-semibold mb-2">Lag (Ingress vs Egress)</h2>
          <div className="h-64 flex items-center justify-center bg-gray-100">
            [Lag Chart Placeholder]
          </div>
        </div>
      </div>
      
      <div className="mt-8 border p-4 rounded shadow">
        <h2 className="text-lg font-semibold mb-4">Pipelines</h2>
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Source</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
             {/* Rows will be dynamically loaded here */}
             <tr>
               <td className="px-6 py-4 whitespace-nowrap" colSpan={4}>No pipelines found.</td>
             </tr>
          </tbody>
        </table>
      </div>
    </div>
  )
}
