import { Truck } from './fleet';

const API_BASE = (typeof window !== 'undefined' && (window as any).__TRANSIT_FLOW_API_URL__) || 'http://localhost:8000';

export interface HealthResponse {
  service: string;
  organization: string;
  status: string;
  version: string;
  database: string;
}

export interface AnalyticsResponse {
  available_trucks: number;
  total_trucks: number;
  available_capacity_tons: number;
  total_capacity_tons: number;
  allocated_capacity_tons: number;
  fleet_utilization_pct: number;
  active_routes_count: number;
  active_bookings_count: number;
  weekly_trend: Array<{ day: string; capacity_tons: number; load_pct: number }>;
}

export interface ReservationResponse {
  booking_id: string;
  truck_id: string;
  client_name: string;
  weight_tons: number;
  remaining_capacity: number;
  truck_status: string;
  total_price_zar: number;
  status: string;
}

export async function checkBackendConnection(): Promise<boolean> {
  try {
    const res = await fetch(`${API_BASE}/api/health`, { method: 'GET', signal: AbortSignal.timeout(1500) });
    return res.ok;
  } catch {
    return false;
  }
}

export async function fetchBackendTrucks(): Promise<Truck[] | null> {
  try {
    const res = await fetch(`${API_BASE}/api/trucks`, { signal: AbortSignal.timeout(2000) });
    if (!res.ok) return null;
    const data = await res.json();
    return data.trucks || [];
  } catch {
    return null;
  }
}

export async function reserveCapacityApi(truckId: string, weightTons: number, clientName = 'Tintswalo Leteane'): Promise<ReservationResponse | null> {
  try {
    const res = await fetch(`${API_BASE}/api/trucks/${truckId}/reserve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ weight_tons: weightTons, client_name: clientName }),
    });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function createTruckApi(truckData: Partial<Truck>): Promise<Truck | null> {
  try {
    const res = await fetch(`${API_BASE}/api/trucks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(truckData),
    });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function fetchAnalyticsApi(): Promise<AnalyticsResponse | null> {
  try {
    const res = await fetch(`${API_BASE}/api/analytics`, { signal: AbortSignal.timeout(2000) });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}

export async function submitSupportApi(message: string, name = 'Workspace User', email = 'admin@leteane-industries.co.za'): Promise<boolean> {
  try {
    const res = await fetch(`${API_BASE}/api/support`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message, name, email }),
    });
    return res.ok;
  } catch {
    return false;
  }
}
