import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle, Pause } from "lucide-react";
import { useState } from "react";
import { type PausePipelineResult, pipelinesApi } from "@/api/pipelines";
import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
	DialogTrigger,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";

/** Plan section 2 / OQ-3: the pause timer's hard ceiling. */
const MAX_PAUSE_MINUTES = 4 * 60;

/**
 * A handful of sane presets plus a free-form minutes input, rather than a
 * duration-string text box -- the ceiling is easy to bump into by accident
 * when typing "4h30m", and this makes the 4h ceiling visible as a limit
 * rather than a validation error.
 */
const PRESETS_MINUTES = [15, 30, 60, 120, 240];

function formatDuration(minutes: number): string {
	const h = Math.floor(minutes / 60);
	const m = minutes % 60;
	if (h === 0) return `${m}m`;
	if (m === 0) return `${h}h`;
	return `${h}h ${m}m`;
}

function toTTL(minutes: number): string {
	return `${minutes}m`;
}

interface PauseDialogProps {
	pipelineId: string;
	/**
	 * When set, the trigger renders as "Extend pause" instead of "Pause" --
	 * this is the same dialog and the same pause() call, just re-invoked
	 * with a fresh ttl (plan: "extending a pause must be trivial").
	 */
	isExtend?: boolean;
	disabled?: boolean;
}

export function PauseDialog({
	pipelineId,
	isExtend,
	disabled,
}: PauseDialogProps) {
	const queryClient = useQueryClient();
	const [open, setOpen] = useState(false);
	const [minutes, setMinutes] = useState(60);
	const [result, setResult] = useState<PausePipelineResult | null>(null);

	const pauseMutation = useMutation({
		mutationFn: () => pipelinesApi.pause(pipelineId, toTTL(minutes)),
		onSuccess: (data) => {
			setResult(data);
			queryClient.invalidateQueries({ queryKey: ["pipeline", pipelineId] });
		},
	});

	const resumeAt = new Date(Date.now() + minutes * 60_000);

	// Plan section 5: "project the breach and show it before the pause is
	// confirmed" -- read-only, re-evaluated as the slider/presets change,
	// via the dedicated GET endpoint rather than waiting for the eventual
	// POST /pause response. Only fetched while the dialog is open and no
	// pause has been committed yet; once `result` is set the post-commit
	// `warning` on the response takes over (see the result panel below).
	const { data: projection, isError: projectionErrored } = useQuery({
		queryKey: ["pause-projection", pipelineId, toTTL(minutes)],
		queryFn: () => pipelinesApi.pauseProjection(pipelineId, toTTL(minutes)),
		enabled: open && !result,
		// The projection only depends on server-side WAL state, which
		// doesn't move fast enough to justify refetching on every render;
		// re-derive it from the query key (id + ttl) changing instead.
		staleTime: 30_000,
		retry: false,
	});

	function handleOpenChange(next: boolean) {
		setOpen(next);
		if (!next) {
			// Reset for the next time the dialog opens, but only after it has
			// closed -- resetting mid-animation would blank the result panel
			// while it is still visible.
			setResult(null);
			pauseMutation.reset();
		}
	}

	return (
		<Dialog open={open} onOpenChange={handleOpenChange}>
			<DialogTrigger asChild>
				<Button variant="outline" disabled={disabled}>
					<Pause className="mr-2 h-4 w-4" />
					{isExtend ? "Extend pause" : "Pause"}
				</Button>
			</DialogTrigger>
			<DialogContent>
				<DialogHeader>
					<DialogTitle>
						{isExtend ? "Extend pause" : "Pause pipeline"}
					</DialogTitle>
					<DialogDescription>
						Stops consuming while retaining the replication slot. WAL
						accumulates on the source until this pipeline resumes or the timer
						below expires, whichever comes first.
					</DialogDescription>
				</DialogHeader>

				{!result ? (
					<div className="space-y-4 py-2">
						<div className="space-y-2">
							<Label htmlFor="pause-ttl">Pause for</Label>
							<div className="flex flex-wrap gap-2">
								{PRESETS_MINUTES.map((preset) => (
									<Button
										key={preset}
										type="button"
										size="sm"
										variant={minutes === preset ? "default" : "outline"}
										onClick={() => setMinutes(preset)}
									>
										{formatDuration(preset)}
									</Button>
								))}
							</div>
							<input
								id="pause-ttl"
								type="range"
								min={5}
								max={MAX_PAUSE_MINUTES}
								step={5}
								value={minutes}
								onChange={(e) => setMinutes(Number(e.target.value))}
								className="w-full"
							/>
							<p className="text-sm text-muted-foreground">
								Resumes automatically at{" "}
								<span className="font-medium text-foreground">
									{resumeAt.toLocaleString()}
								</span>{" "}
								(in {formatDuration(minutes)}) unless the WAL budget guard trips
								first. Ceiling: 4h.
							</p>
						</div>

						{projection?.warning && (
							<div
								className="flex gap-2 rounded-md border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-700 dark:text-amber-400"
								data-testid="pause-projection-warning"
							>
								<AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
								<span>{projection.warning}</span>
							</div>
						)}

						{/* The projection is safety-critical: if it errors, an operator
						    must not be left reading "no warning shown" as "this pause
						    is safe" when the truth is "we couldn't check". Fail
						    visibly instead of silently rendering nothing. */}
						{projectionErrored && (
							<div
								className="flex gap-2 rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive"
								data-testid="pause-projection-unavailable"
							>
								<AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
								<span>
									Couldn't check the WAL-budget projection for this pause.
									Proceed with caution.
								</span>
							</div>
						)}

						{pauseMutation.isError && (
							<p className="text-sm text-destructive">
								{(pauseMutation.error as Error).message}
							</p>
						)}
					</div>
				) : (
					<div className="space-y-3 py-2" data-testid="pause-result">
						<p className="text-sm">
							Paused. Resumes automatically at{" "}
							<span className="font-medium">
								{result.pausedUntil
									? new Date(result.pausedUntil).toLocaleString()
									: "—"}
							</span>
							.
						</p>
						{result.warning && (
							<div
								className="flex gap-2 rounded-md border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-amber-700 dark:text-amber-400"
								data-testid="pause-breach-warning"
							>
								<AlertTriangle className="h-4 w-4 shrink-0 mt-0.5" />
								<span>{result.warning}</span>
							</div>
						)}
					</div>
				)}

				<DialogFooter>
					{!result ? (
						<Button
							onClick={() => pauseMutation.mutate()}
							disabled={pauseMutation.isPending}
						>
							{pauseMutation.isPending ? "Pausing…" : "Confirm pause"}
						</Button>
					) : (
						<Button onClick={() => handleOpenChange(false)}>Done</Button>
					)}
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}
