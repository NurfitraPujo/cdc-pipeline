import { X } from "lucide-react";
import { useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
	bareTableName,
	groupTablesBySchema,
	schemaOf,
} from "@/lib/tableGrouping";

/**
 * Structured editor for the `nats/protobuf` transformer's options.
 *
 * Mirrors `NewNatsProtoTransformer` in internal/transformer/nats/protobuf.go:
 *
 *  - `nats_url` and `subject` are required; construction fails without them.
 *  - At least one of `schemas` / `tables` is required. An unfiltered instance
 *    forwards every table to the responder, where they all fail metadata
 *    lookup (WS-8 item 4).
 *  - `timeout_ms` defaults to `DefaultTimeoutMs(batch_size)` — a 15s floor,
 *    growing 5ms per record once the batch exceeds 3000.
 *  - `batch_size` and `pipeline_id` are injected by engine/factory.go and are
 *    deliberately not editable here; setting them by hand only overrides
 *    values the worker already knows.
 *
 * The raw JSON editor remains available underneath for anything this form
 * does not model — processor options are an opaque map on the wire.
 */

/** Matches DefaultTimeoutMs in internal/transformer/nats/protobuf.go. */
export const NATS_TIMEOUT_FLOOR_MS = 15000;

export interface NatsProtobufOptionsValue {
	nats_url?: string;
	subject?: string;
	timeout_ms?: number;
	schemas?: string[];
	tables?: string[];
	[key: string]: unknown;
}

interface NatsProtobufOptionsProps {
	value: NatsProtobufOptionsValue;
	onChange: (next: NatsProtobufOptionsValue) => void;
	/** Schemas configured on the pipeline's source, offered as suggestions. */
	availableSchemas?: string[];
	/**
	 * Tables discovered for the source, as `schema.table` (or a bare name for
	 * public) — the same shape `sourcesApi.getTables` returns.
	 */
	availableTables?: string[];
	index: number;
}

export function NatsProtobufOptions({
	value,
	onChange,
	availableSchemas = [],
	availableTables = [],
	index,
}: NatsProtobufOptionsProps) {
	const schemas = value.schemas ?? [];
	const tables = value.tables ?? [];

	const setField = (key: string, next: unknown) => {
		const draft = { ...value };
		if (next === undefined || next === "") {
			delete draft[key];
		} else {
			draft[key] = next;
		}
		onChange(draft);
	};

	const toggleInList = (key: "schemas" | "tables", entry: string) => {
		const current = (value[key] as string[] | undefined) ?? [];
		const next = current.includes(entry)
			? current.filter((e) => e !== entry)
			: [...current, entry];
		setField(key, next.length > 0 ? next : undefined);
	};

	// Schemas actually present in the discovered tables, unioned with whatever
	// the source declares, so the suggestions cover both.
	const schemaSuggestions = Array.from(
		new Set([...availableSchemas, ...availableTables.map(schemaOf)]),
	).sort();

	// Grouped so a table can be picked within the schema it belongs to. Note
	// the stored value is the BARE name: matchesTable in protobuf.go compares
	// against m.Table and ignores the schema entirely.
	const grouped = groupTablesBySchema(availableTables);
	const groupedSchemas = [...grouped.keys()];

	const hasFilter = schemas.length > 0 || tables.length > 0;

	return (
		<div className="space-y-4 rounded-lg border p-4">
			<div className="grid gap-4 md:grid-cols-2">
				<div className="space-y-2">
					<Label htmlFor={`nats-url-${index}`}>NATS URL</Label>
					<Input
						id={`nats-url-${index}`}
						value={(value.nats_url as string) ?? ""}
						placeholder="nats://localhost:4222"
						onChange={(e) => setField("nats_url", e.target.value)}
					/>
					{!value.nats_url && (
						<p className="text-xs text-destructive">
							Required — the transformer fails to start without it.
						</p>
					)}
				</div>

				<div className="space-y-2">
					<Label htmlFor={`nats-subject-${index}`}>Subject</Label>
					<Input
						id={`nats-subject-${index}`}
						value={(value.subject as string) ?? ""}
						placeholder="daya.core.transform"
						onChange={(e) => setField("subject", e.target.value)}
					/>
					{!value.subject && (
						<p className="text-xs text-destructive">
							Required — the transformer fails to start without it.
						</p>
					)}
				</div>

				<div className="space-y-2">
					<Label htmlFor={`nats-timeout-${index}`}>Request timeout (ms)</Label>
					<Input
						id={`nats-timeout-${index}`}
						type="number"
						min={1}
						value={value.timeout_ms ?? ""}
						placeholder={String(NATS_TIMEOUT_FLOOR_MS)}
						onChange={(e) =>
							setField(
								"timeout_ms",
								e.target.value === ""
									? undefined
									: Number.parseInt(e.target.value, 10),
							)
						}
					/>
					<p className="text-xs text-muted-foreground">
						Leave blank to scale with batch size ({NATS_TIMEOUT_FLOOR_MS / 1000}
						s floor).
					</p>
				</div>
			</div>

			<div className="rounded-md border border-amber-500/40 bg-amber-500/10 p-3 text-xs">
				<p className="font-medium">Schemas and tables combine with OR.</p>
				<p className="mt-1 text-muted-foreground">
					A record is transformed if its schema is listed <em>or</em> its table
					is listed — not both. So <code>schemas: custom_objects</code> plus{" "}
					<code>tables: visitations</code> sends every{" "}
					<code>custom_objects</code> row <em>and</em> every{" "}
					<code>visitations</code> row, including{" "}
					<code>public.visitations</code>. Table names are matched without their
					schema.
				</p>
			</div>

			<div className="space-y-2">
				<Label>Schemas</Label>
				{schemaSuggestions.length > 0 ? (
					<div className="flex flex-wrap gap-2">
						{schemaSuggestions.map((s) => (
							<button
								key={s}
								type="button"
								onClick={() => toggleInList("schemas", s)}
								aria-pressed={schemas.includes(s)}
								className={`inline-flex items-center gap-1 rounded-full border px-3 py-1 text-sm transition-colors ${
									schemas.includes(s)
										? "bg-primary text-primary-foreground border-primary"
										: "bg-background hover:bg-muted"
								}`}
							>
								{s}
								{schemas.includes(s) && <X className="h-3 w-3" />}
							</button>
						))}
					</div>
				) : (
					<FreeTextList
						id={`nats-schemas-${index}`}
						placeholder="Add a schema, e.g. custom_objects"
						entries={schemas}
						onChange={(next) =>
							setField("schemas", next.length > 0 ? next : undefined)
						}
					/>
				)}
			</div>

			<div className="space-y-2">
				<Label>Tables</Label>
				{groupedSchemas.length > 0 ? (
					<div className="space-y-3">
						{groupedSchemas.map((schema) => (
							<div key={schema}>
								<p className="mb-1 text-xs font-medium text-muted-foreground">
									{schema}
								</p>
								<div className="flex flex-wrap gap-2">
									{(grouped.get(schema) ?? []).map(bareTableName).map((t) => (
										<button
											key={`${schema}.${t}`}
											type="button"
											onClick={() => toggleInList("tables", t)}
											aria-pressed={tables.includes(t)}
											className={`inline-flex items-center gap-1 rounded-full border px-3 py-1 text-sm transition-colors ${
												tables.includes(t)
													? "bg-primary text-primary-foreground border-primary"
													: "bg-background hover:bg-muted"
											}`}
										>
											{t}
											{tables.includes(t) && <X className="h-3 w-3" />}
										</button>
									))}
								</div>
							</div>
						))}
					</div>
				) : (
					<FreeTextList
						id={`nats-tables-${index}`}
						placeholder="Add a table name, e.g. visitations"
						entries={tables}
						onChange={(next) =>
							setField("tables", next.length > 0 ? next : undefined)
						}
					/>
				)}
			</div>

			{!hasFilter && (
				<p className="text-xs text-destructive">
					Set at least one schema or table. An unfiltered transformer forwards
					every table to the responder, where they all fail metadata lookup.
				</p>
			)}
		</div>
	);
}

/** Tag input for when no discovered values are available to pick from. */
function FreeTextList({
	id,
	placeholder,
	entries,
	onChange,
}: {
	id: string;
	placeholder: string;
	entries: string[];
	onChange: (next: string[]) => void;
}) {
	const [draft, setDraft] = useState("");

	const add = () => {
		const trimmed = draft.trim();
		if (trimmed && !entries.includes(trimmed)) {
			onChange([...entries, trimmed]);
		}
		setDraft("");
	};

	return (
		<div className="space-y-2">
			{entries.length > 0 && (
				<div className="flex flex-wrap gap-2">
					{entries.map((e) => (
						<Badge key={e} variant="secondary" className="gap-1">
							{e}
							<button
								type="button"
								aria-label={`Remove ${e}`}
								onClick={() => onChange(entries.filter((x) => x !== e))}
							>
								<X className="h-3 w-3" />
							</button>
						</Badge>
					))}
				</div>
			)}
			<div className="flex gap-2">
				<Input
					id={id}
					value={draft}
					placeholder={placeholder}
					onChange={(e) => setDraft(e.target.value)}
					onKeyDown={(e) => {
						if (e.key === "Enter") {
							e.preventDefault();
							add();
						}
					}}
				/>
				<Button type="button" variant="outline" onClick={add}>
					Add
				</Button>
			</div>
		</div>
	);
}
