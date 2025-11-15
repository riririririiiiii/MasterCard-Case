import TableResult from "./TableResult";

export default function ResponseBox({ data }) {
  if (data.type === "text") {
    return <p>{data.message}</p>;
  }

  if (data.type === "table") {
    return <TableResult headers={data.headers} rows={data.rows} />;
  }

  return <p>❓ Unknown response type</p>;
}