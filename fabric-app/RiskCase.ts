import { entity, field, type } from "@microsoft/rayfin-core";

@entity()
export class RiskCase {
  @field({ type: type.uuid() })
  id!: string;

  @field({ type: type.text({ maxLength: 200 }), required: true })
  patient_name!: string;

  @field({ type: type.text({ maxLength: 50 }), required: true })
  patient_id!: string;

  @field({ type: type.text({ maxLength: 100 }), required: true })
  facility!: string;

  @field({ type: type.text({ maxLength: 100 }) })
  primary_diagnosis!: string;

  @field({
    type: type.set(["Critical", "High", "Medium", "Low"]),
    required: true,
  })
  priority!: string;

  @field({
    type: type.set([
      "Open",
      "In Progress",
      "Scheduled Follow-Up",
      "Resolved",
    ]),
    required: true,
  })
  status!: string;

  @field({ type: type.text({ maxLength: 100 }) })
  assigned_to!: string;

  @field({ type: type.text({ maxLength: 500 }) })
  notes!: string;

  @field({ type: type.text({ maxLength: 500 }) })
  resolution_notes!: string;

  @field({ type: type.date(), required: true })
  created_at!: Date;

  @field({ type: type.date() })
  resolved_at!: Date;

  @field({ type: type.date() })
  due_date!: Date;

  @field({ type: type.integer() })
  prior_admissions_12m!: number;

  @field({ type: type.text({ maxLength: 100 }), required: true })
  user_id!: string;
}
