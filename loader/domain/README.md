# Domain Model Notes

## Officers

Officers are the most critical entity in the NPDC graph. They are the primary focus of the application, and they are the most complex entity to model. The loader must resolve officer identity across multiple sources, and it must preserve source-specific representations of officers.

In addition, officer data is often incomplete or inconsistent across sources. The loader must be able to handle missing data, conflicting data, and data that changes over time. It must also be able to handle manual review and overrides of source data.

We accomplish these goals while maintaining data provenance by modeling officer identity and officer attributes separately. The `Officer` node which is used in the graph is not updated directly by individual sources. Instead, we compose the node from source-specific records.

### Officer Composition

#### Officer Node

Each `Officer` node represents a unique law enforcement officer. This is the node that is connected in the graph to law enforcement agencies that employ the officer and allegations made against the officer.

Updates to the properties of this node are protected so that multiple sources can contribute to the node without overwriting each other's data. We collapse source-specific representations of officers into a single `Officer` so that the graph can be queried for a single officer identity quickly and efficiently at runtime without needing to traverse multiple source records. The loader and the application are responsible for keeping the `Officer` node up to date when source records change.

**Officer Properties**

An Officer nodes properties provide a composed view of the officer's attributes such as `first_name`, `last_name`, `gender`, and `year_of_birth`. These allow the application to display and search for officers without needing to traverse source records. 

#### StateID Node

While most nodes in the graph are unique by their own properties, `Officer` nodes are unique by their relationships to `StateID` nodes. Each `StateID` is a unique identifier in a namespace, and each `Officer` is a unique person. An `Officer` may have multiple `StateID`s if that person has been registered as an officer in multiple states or is tracked by multiple data sources.

The `StateID` node represents a unique identifier for an officer. We can illustrate what this means by using New York State as an example. In New York, the state tracks officers by their Tax ID. This means that each officer in New York has a unique Tax ID that is only ever assigned to that officer. If we have two officers with the same Tax ID, we know that they are the same person, even if they have different names or other attributes.

Each `StateID` has a `state`, `id_name`, and `value` property. For a given state and id_name, the value must be unique. For example, there can only be one `StateID` with `state=IL`, `id_name=NPI ID`, and `value=1234567890`. In this way, a specific `state` and `id_name` combination defines a namespace, and the `value` is the unique identifier within that namespace. The loader uses this uniqueness to resolve officer identity across sources.


#### StateIDNamespace Node

To avoid data collisions between sources, we require that each namespace be owned by a single source. This means that one source is responsible to creating unique `value`s for a given `state` and `id_name`. Other sources may only update values in that namespace if they have been granted permission by the owner.

The `StateIDNamespace` node represents this identifier domain. It is connected to its `StateID` nodes through the `IN_NAMESPACE` relationship. Sources claim authority over namespaces through the `CLAIMS_ID_NAMESPACE` relationship, which can mark the source as an `OWNER`, `STEWARD`, or `CONTRIBUTOR`.

#### OfficerRecord Node

The `OfficerRecord` node represents a data record for an officer provided by a specific source. It contains the officer's attributes as reported by that source. In addition to the property values provided by the source, the `OfficerRecord` node also contains metadata about the record, such as the specific state ID linked to the record and whether the values have been manually reviewed or locked by a source member.

#### OfficerCanonicalField Node

The `OfficerCanonicalField` node represents the selected source record for a specific officer property. It is connected to the `Officer` node through the `HAS_CANONICAL_FIELD` relationship and to the selected `OfficerRecord` through the `SELECTED_FROM_RECORD` relationship. The `field_name` property identifies the officer property, and the `policy` property records why that record was selected.



### Officer Updates

When a source provides a data update for an officer, the loader updates the source's `OfficerRecord`. After updating the record, the loader updates the `Officer` node based on the active `OfficerCanonicalField` selections.

#### Source-Specific Officer Records

Different sources may represent the same officer differently. The loader should not resolve those differences by letting the last loaded source overwrite shared `Officer` properties.

`OfficerRecord` stores a source-specific representation of an officer:

```text
(:OfficerRecord)-[:DESCRIBES_OFFICER]->(:Officer)
(:OfficerRecord)-[:FROM_SOURCE]->(:Source)
(:OfficerRecord)-[:USES_STATE_ID]->(:StateID)
```

Source casing, spelling, and categorical labels should be preserved exactly. Matching code may compute normalized comparison keys, but those keys should not replace the source value. For example, `McNair`, `Mcnair`, `MC NAIR`, and `MCNAIR` may compare similarly while remaining distinct stored representations. Likewise, one source may store `M` while another stores `Male`.


#### Canonical Officer Fields

`Officer` properties such as `first_name`, `last_name`, `gender`, and `year_of_birth` are denormalized display/search fields. The source of truth for each public property is an active `OfficerCanonicalField`.

```text
(:Officer)-[:HAS_CANONICAL_FIELD]->(:OfficerCanonicalField {field_name, value, policy, is_active})
(:OfficerCanonicalField)-[:CANONICAL_FIELD_FOR]->(:Officer)
(:OfficerCanonicalField)-[:SELECTED_FROM_RECORD]->(:OfficerRecord)
```

An `OfficerCanonicalField` says which `OfficerRecord` supplies one Officer property. Its `field_name` identifies the property to read from the selected record, and `policy` records why that record was selected, such as `SOURCE_PRIORITY`, `MANUAL_SELECTION`, `MOST_COMPLETE`, or `MOST_RECENT`.

Loader behavior should follow this rule:

```text
After updating OfficerRecord values, refresh active OfficerCanonicalField values from their selected OfficerRecord fields, then refresh the denormalized Officer properties from active OfficerCanonicalField values.
```

This keeps transmission from source records to the shared `Officer` node explicit. The `Officer` node does not decide which record wins; canonical field selections do.

#### Manual Review Locks

Some updates come from automated loader runs. Others come from users acting on behalf of a source. If a source member manually reviews a field on that source's `OfficerRecord`, the review should be durable.

`OfficerRecordFieldReview` records field-level review metadata:

```text
(:OfficerRecord)-[:HAS_FIELD_REVIEW]->(:OfficerRecordFieldReview {field_name, is_active, lock_loader_updates})
(:OfficerRecordFieldReview)-[:REVIEWED_BY]->(:User)
(:OfficerRecordFieldReview)-[:REVIEWED_FOR_SOURCE]->(:Source)
```

Loader behavior should follow this rule:

```text
If a field has an active review lock, automated loader updates must not overwrite that field.
```

Manual updates may change reviewed fields when the user is authorized to act for the source. Clearing or replacing a review lock should itself be auditable.


### Officer Identity Merging

In cases where a single officer has multiple `StateID`s, we need to provide a way to merge these identities so that we have a complete view of the officer's career. We can accomplish this by merging the identities in the graph so that we can compose the data attributed to each `StateID` into a single record at query time. This can be done in two ways:

**Authoritative merge**
When two identifiers are known authoritatively to describe the same person, both `StateID` nodes should attach to the same `Officer`. This structural form is the authoritative merge. Future updates to any of the attached `StateID` will be reflected in the same `Officer` node, and the application can use that single node for and graph relationships that involve that officer.

Two StateIDs can only be merged authoritatively if the owners of both namespaces agree that the two identifiers describe the same person. This is represented by one source giving the other source permission to assert an identity link in a given namespace.

**Identity assertion**  
When two identifiers are not known authoritatively to describe the same person, we can create an `IdentityAssertion` node that records that a system, source, or user believes two `StateID` nodes may describe the same person. Based on the strength of the assertion and the goal of the query, the application may choose to treat the two identifiers as the same person or as separate people.
