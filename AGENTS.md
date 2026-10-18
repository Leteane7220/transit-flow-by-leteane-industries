<!-- LOVABLE:BEGIN -->
> [!IMPORTANT]
> This project is connected to [Lovable](https://lovable.dev). Avoid rewriting
> published git history — force pushing, or rebasing/amending/squashing commits
> that are already pushed — as it rewrites history on Lovable's side and the
> user will likely lose their project history.
>
> Commits you push to the connected branch sync back to Lovable and show up in
> the editor, so keep the branch in a working state.
<!-- LOVABLE:END -->

## Transit Flow architecture
- Keep fleet sample data and filtering in a browser safe module so dashboard interactions work without a connected service.
- Define dashboard styling through semantic tokens in the global stylesheet so the palette remains consistent.
- Keep this initial dashboard at the index route with in session interactions; connecting persistent fleet records requires Lovable Cloud.
