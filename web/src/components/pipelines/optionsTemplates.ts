export interface ProcessorConfigShape {
	name: string;
	type: string;
	options?: Record<string, unknown>;
	operationTypes?: string[];
}

export const MASK_DEFAULT: ProcessorConfigShape = {
	name: "",
	type: "mask",
	operationTypes: ["insert", "update"],
	options: { fields: ["email"], salt: "" },
};

export const UPPERCASE_DEFAULT: ProcessorConfigShape = {
	name: "",
	type: "uppercase",
	operationTypes: ["insert", "update"],
	options: { column: "name" },
};

export const CUSTOM_DEFAULT: ProcessorConfigShape = {
	name: "",
	type: "custom",
	operationTypes: [],
	options: {},
};

export const TEMPLATES_BY_TYPE: Record<
	string,
	readonly ProcessorConfigShape[]
> = {
	mask: [MASK_DEFAULT],
	uppercase: [UPPERCASE_DEFAULT],
	custom: [CUSTOM_DEFAULT],
};
