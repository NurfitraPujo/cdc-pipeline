import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/dashboard')({
  component: DashboardPage,
})

function DashboardPage() {
  return (
    <div className="page-wrap px-4 pb-8 pt-14">
      <h1 className="text-3xl font-bold">Dashboard</h1>
      <p className="mt-4 text-muted-foreground">Welcome to the CDC Pipeline Dashboard.</p>
    </div>
  )
}
