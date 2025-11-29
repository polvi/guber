// Global env reference - will be set by the worker
let globalEnv: any = null;

export const setEnv = (env: any) => {
  globalEnv = env;
};

const getBody = async <T>(response: Response): Promise<T> => {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    return text as unknown as T;
  }
};

export const customFetch = async <T>(
  url: string,
  options: RequestInit,
): Promise<T> => {
  if (!globalEnv) {
    throw new Error('Environment not set. Call setEnv() first.');
  }
  
  const request = new Request(url, options);
  const response = await globalEnv.GUBER_API.fetch(request);
  const data = await getBody<T>(response);
  return { status: response.status, data, headers: response.headers } as T;
};
