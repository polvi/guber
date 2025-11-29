export const customFetch = async <T>(
  url: string,
  options: RequestInit,
): Promise<T> => {
  const request = new Request(url, options);
  const response = await env.GUBER_API.fetch(request);
  const data = await getBody<T>(response);
  return { status: response.status, data } as T;
};
