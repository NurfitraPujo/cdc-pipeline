# Reusable UI Components & Design System

The `src/components/` directory houses the project's user interface layer, split into generic design primitives and domain-specific feature components.

## Directory Structure

- **`ui/`**: Low-level UI primitives built on top of standard Radix UI patterns (styled with Tailwind CSS). Contains foundational primitives:
  - `Button`, `Input`, `Textarea`, `Label`, `Select`
  - `Card`, `Dialog`, `DropdownMenu`
  - `Table`, `Badge`, `Alert`
- **`layout/`**: Structural elements such as wrapper containers and grid containers for visual consistency across pages.

## Key Feature Components

- **`ConfigEditor.tsx`**: A YAML editing workspace with change tracking and save capabilities, used for updating global properties or pipeline-specific configuration definitions.
- **`PipelineTable.tsx`**: A dashboard table displaying active pipelines, utilizing TanStack Table for efficient sorting, paginating, and filtering. Includes visual indicators for real-time status and operational LSN lag.
- **`MetricCard.tsx`**: Summary cards for dashboard metrics, featuring progress metrics, trending indicators, and highlight colors.
- **`StatusBadge.tsx`**: An atomic status indicator mapped to pipeline and worker states (e.g., green for `Ready`/`Running`, amber for `Transitioning`, and red for `Error`).
- **`Header.tsx` & `Footer.tsx`**: The global application navigation bar and page footer, featuring a user profile action block, a connection/health status badge, and theme toggling.
- **`ThemeToggle.tsx`**: Switcher enabling seamless transitions between Dark Mode and Light Mode, applying CSS class changes via tailwind theme extensions.

## Design Conventions

1. **Accessibility (a11y)**: Focus states, hover outlines, correct ARIA attributes, and semantic HTML elements must be maintained throughout UI development.
2. **Micro-animations**: Dynamic hover interactions and loading skeletons should be included for elements such as buttons, form fields, and status badges.
3. **Consistency**: Color semantics should always adhere to system configurations:
   - **Primary**: Brand identifier (indigo/violet hues)
   - **Success (Healthy)**: Green color variants
   - **Warning (Transitioning)**: Yellow/Amber color variants
   - **Destructive (Error)**: Red/Rose color variants
