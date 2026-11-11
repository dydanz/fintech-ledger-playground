import { Link } from 'react-router';

export function NotFound() {
  return (
    <section>
      <h1>Page not found</h1>
      <p className="sub">
        Nothing lives at this address. <Link to="/">Back to the monitor</Link>
      </p>
    </section>
  );
}
