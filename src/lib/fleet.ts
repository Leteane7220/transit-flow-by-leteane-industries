export type Truck = { id: string; name: string; plate: string; type: string; origin: string; destination: string; capacity: number; total: number; status: 'Available' | 'On route' | 'Fully booked'; departure: string; driver: string };
export const trucks: Truck[] = [
 { id:'TF1024', name:'Volvo FH 540', plate:'GP 4821', type:'Curtain side', origin:'Johannesburg', destination:'Cape Town', capacity:24, total:34, status:'Available', departure:'Today, 14:30', driver:'Thabo Mokoena' },
 { id:'TF1025', name:'Scania R500', plate:'GP 6392', type:'Refrigerated', origin:'Durban', destination:'Johannesburg', capacity:18, total:28, status:'Available', departure:'Today, 16:00', driver:'Sipho Dlamini' },
 { id:'TF1026', name:'Mercedes Actros', plate:'CA 2106', type:'Flatbed', origin:'Cape Town', destination:'Port Elizabeth', capacity:8, total:32, status:'On route', departure:'Today, 12:00', driver:'David Petersen' },
 { id:'TF1027', name:'MAN TGX 480', plate:'GP 8843', type:'Curtain side', origin:'Johannesburg', destination:'Durban', capacity:30, total:34, status:'Available', departure:'Tomorrow, 08:00', driver:'James Nkosi' },
 { id:'TF1028', name:'Volvo FH 460', plate:'ND 3927', type:'Refrigerated', origin:'Durban', destination:'Cape Town', capacity:0, total:28, status:'Fully booked', departure:'Today, 18:00', driver:'Bongani Zulu' },
 { id:'TF1029', name:'Scania S560', plate:'GP 5710', type:'Flatbed', origin:'Johannesburg', destination:'Pretoria', capacity:22, total:32, status:'Available', departure:'Tomorrow, 09:30', driver:'Peter Molefe' },
];
export function filterTrucks(items: Truck[], search: string, origin: string, destination: string, type: string, status: string) {
 return items.filter(t => `${t.name} ${t.id} ${t.origin} ${t.destination}`.toLowerCase().includes(search.toLowerCase()) && (!origin || t.origin === origin) && (!destination || t.destination === destination) && (!type || t.type === type) && (!status || t.status === status));
}
