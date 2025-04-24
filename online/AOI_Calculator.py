import json,sys

def aoi_avg(aoi_fn:str,T=None)->float:
    if not aoi_fn.endswith('.aoi'):
        return
    try:
        with open(aoi_fn,'r',encoding="utf-8") as aoi:
            data = json.load(aoi)
    except Exception as e:
        print(f"error while calculating aoi avg for {aoi_fn}:{e}")
        return
    
    delta_0,ts = data["delta"],data["time_sequence"]
    if len(ts)==0:
        return None
    if T is None:
        T = ts[-1][1] # default: t_n^{'}
    
    if T<=ts[0][1]:
        return (2*delta_0+T)*T/2

    # N(T) = max{n|t_n<T}
    def N_T(tp:float)->int:
        n = len(ts)
        l,r = 0,n
        
        ans = l
        while l<r:
            mid = (l+r)>>1
            if ts[mid][0] < tp:
                ans = mid
                l = mid+1
            else:
                r = mid
        return ans
    
    up = N_T(T)
    return ((2*delta_0+ts[0][1])*ts[0][1]-pow(ts[0][1],2)+pow(T-ts[up][0],2))/(2*T) + sum(
        ((ts[i][0]-ts[i-1][0])*(ts[i][1]-ts[i][0]) + pow(ts[i][0]-ts[i-1][0],2)) for i in range(1,up+1)            
    )/T

if __name__ == "__main__":
    print('Usage python3 AOI_Calculator.py /path/to/your/{}.aoi')
    aoi_target = sys.argv[1]
    print(aoi_avg(aoi_target))