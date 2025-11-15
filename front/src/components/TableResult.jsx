import '../styles/styles.css';

export default function TableResult({ headers, rows }) {
  return (
    <table border="1">
      <thead>
        <tr>
          {headers.map((head, idx) => (
            <th key={idx}>{head}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, idx) => (
          <tr key={idx}>
            {row.map((cell, cid) => (
              <td key={cid}>{cell}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}