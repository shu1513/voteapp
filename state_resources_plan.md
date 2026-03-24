1. **Build constants/config first.**
   - `CENSUS_STATES_API`
   - Seed source URLs array
   - `STATE_ABBR_BY_FIPS` map (50 states + DC)

2. **Build Producer.**
   - Fetch Census states
   - Create `state_resources` draft items
   - Push to Redis Stream `staging:pending`
   - Insert `staging_items` with `status = 'pending'`

3. **Build Validator.**
   - Check required fields, URL format, `sources` structure
   - Move valid items to `validated`
   - Move rejected items to `rejected` with reason

4. **Build Writer.**
   - Read validated items
   - Upsert into `state_resources` by `state_fips`
   - Mark `staging_items.status = 'written'`

5. **Add annual scheduler.**
   - Run once per year for all 50 states + DC
