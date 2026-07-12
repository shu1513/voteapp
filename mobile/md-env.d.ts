// babel-plugin-inline-import turns .md imports into their file contents.
declare module "*.md" {
  const content: string;
  export default content;
}
